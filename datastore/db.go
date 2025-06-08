package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	activeName      = "current-data"
	defaultMaxBytes = 10 * 1024 * 1024
)

var (
	ErrNotFound    = fmt.Errorf("record does not exist")
	segRE          = regexp.MustCompile(`^segment-(\d+)\.data$`)
	MaxSegmentSize = int64(defaultMaxBytes)
)

type position struct {
	segID  int
	offset int64
}

type segment struct {
	file *os.File
	id   int
	size int64
	path string
	mu   sync.RWMutex // Per-segment lock for safe concurrent access
}

type entry struct {
	key   string
	value string
}

type writeRequest struct {
	key    string
	value  string
	respCh chan error
}

type DB struct {
	dir      string
	segments []*segment
	active   *segment
	index    map[string]position

	mu      sync.RWMutex
	writeCh chan writeRequest
	quit    chan struct{}
	wg      sync.WaitGroup
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:     dir,
		index:   make(map[string]position),
		quit:    make(chan struct{}),
		writeCh: make(chan writeRequest, 100),
	}

	if err := db.loadSegments(); err != nil {
		return nil, err
	}
	if err := db.recover(); err != nil {
		return nil, err
	}

	db.wg.Add(1)
	go db.writer()
	go db.compactor()
	return db, nil
}

func (db *DB) writer() {
	defer db.wg.Done()
	for {
		select {
		case req := <-db.writeCh:
			err := db.doPut(req.key, req.value)
			req.respCh <- err
		case <-db.quit:
			return
		}
	}
}

func (db *DB) doPut(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if v := os.Getenv("SEG_MAX"); v != "" {
		if n, _ := strconv.ParseInt(v, 10, 64); n > 0 {
			MaxSegmentSize = n
		}
	}

	e := entry{key: key, value: value}
	data := e.Encode()

	offset := db.active.size
	n, err := db.active.file.Write(data)
	if err != nil {
		return err
	}

	// Update segment size
	db.active.size += int64(n)

	// Update index
	db.index[key] = position{
		segID:  -1,
		offset: offset,
	}

	// Check segment size
	if db.active.size >= MaxSegmentSize {
		if err := db.rotateActive(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rotateActive() error {
	// Sync active file
	if err := db.active.file.Sync(); err != nil {
		return err
	}

	// Determine next segment ID
	nextID := 0
	if len(db.segments) > 0 {
		lastID := db.segments[len(db.segments)-1].id
		nextID = lastID + 1
	}

	// Rename active file
	frozenPath := filepath.Join(db.dir, fmt.Sprintf("segment-%d.data", nextID))
	if err := os.Rename(db.active.path, frozenPath); err != nil {
		return err
	}

	// Keep file descriptor open for frozen segment
	frozenFile := db.active.file

	// Add frozen segment
	db.segments = append(db.segments, &segment{
		file: frozenFile,
		id:   nextID,
		size: db.active.size,
		path: frozenPath,
	})

	// Update index
	for key, pos := range db.index {
		if pos.segID == -1 {
			pos.segID = nextID
			db.index[key] = pos
		}
	}

	// Create new active segment
	newActivePath := filepath.Join(db.dir, activeName)
	newActiveFile, err := os.OpenFile(
		newActivePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0o644,
	)
	if err != nil {
		return err
	}

	db.active = &segment{
		file: newActiveFile,
		id:   -1,
		size: 0,
		path: newActivePath,
	}
	return nil
}

func (db *DB) Put(key, value string) error {
	respCh := make(chan error)
	db.writeCh <- writeRequest{
		key:    key,
		value:  value,
		respCh: respCh,
	}
	return <-respCh
}

func (db *DB) Get(key string) (string, error) {
	db.mu.RLock()
	pos, ok := db.index[key]
	if !ok {
		db.mu.RUnlock()
		return "", ErrNotFound
	}
	var s *segment
	if pos.segID == -1 {
		s = db.active
	} else {
		idx := db.segIdx(pos.segID)
		if idx < 0 || idx >= len(db.segments) {
			db.mu.RUnlock()
			return "", fmt.Errorf("invalid segment ID %d", pos.segID)
		}
		s = db.segments[idx]
	}
	// Lock segment for reading
	s.mu.RLock()
	defer s.mu.RUnlock()
	db.mu.RUnlock()

	// Read header: 8 bytes (key len + value len)
	hdr := make([]byte, 8)
	if _, err := s.file.ReadAt(hdr, pos.offset); err != nil {
		return "", fmt.Errorf("failed to read entry header: %w", err)
	}
	kl := binary.LittleEndian.Uint32(hdr[0:4])
	vl := binary.LittleEndian.Uint32(hdr[4:8])
	totalSize := int64(8 + kl + vl)

	// Read full entry
	buf := make([]byte, totalSize)
	copy(buf, hdr)
	if _, err := s.file.ReadAt(buf[8:], pos.offset+8); err != nil {
		return "", fmt.Errorf("failed to read entry body: %w", err)
	}

	var e entry
	if err := e.Decode(buf); err != nil {
		return "", fmt.Errorf("decode error: %w", err)
	}
	return e.value, nil
}

func (db *DB) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, s := range append(db.segments, db.active) {
		total += s.size
	}
	return total, nil
}

func (db *DB) Close() error {
	close(db.quit)
	close(db.writeCh)
	db.wg.Wait()

	var first error
	for _, s := range append(db.segments, db.active) {
		if err := s.file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (db *DB) loadSegments() error {
	ents, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	var ids []int
	for _, e := range ents {
		if e.IsDir() || e.Name() == activeName {
			continue
		}
		if m := segRE.FindStringSubmatch(e.Name()); len(m) == 2 {
			id, _ := strconv.Atoi(m[1])
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)

	for _, id := range ids {
		p := filepath.Join(db.dir, fmt.Sprintf("segment-%d.data", id))
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		st, _ := f.Stat()
		db.segments = append(db.segments, &segment{file: f, id: id, size: st.Size(), path: p})
	}

	p := filepath.Join(db.dir, activeName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	st, _ := f.Stat()
	db.active = &segment{file: f, id: -1, size: st.Size(), path: p}
	return nil
}

func (db *DB) recover() error {
	for _, s := range append(db.segments, db.active) {
		if err := db.scanSegment(s); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) scanSegment(s *segment) error {
	r := bufio.NewReader(s.file)
	offset := int64(0)
	for {
		var e entry
		n, err := e.DecodeFromReader(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		db.index[e.key] = position{segID: s.id, offset: offset}
		offset += int64(n)
	}
	return nil
}

func (db *DB) compactor() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			_ = db.merge()
		case <-db.quit:
			ticker.Stop()
			return
		}
	}
}

func (db *DB) Merge() error {
	return db.merge()
}

func (db *DB) merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.segments) < 2 {
		return nil
	}

	// Find max segment ID for merged segment
	maxID := -1
	for _, s := range db.segments {
		if s.id > maxID {
			maxID = s.id
		}
	}
	mergedID := maxID + 1

	tmp := filepath.Join(db.dir, fmt.Sprintf("merge-tmp-%d.data", time.Now().UnixNano()))
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer tf.Close()

	seen := make(map[string]struct{})
	for i := len(db.segments) - 1; i >= 0; i-- {
		if err := db.copyUnique(db.segments[i], tf, seen); err != nil {
			return err
		}
	}
	tf.Sync()

	mergedPath := filepath.Join(db.dir, fmt.Sprintf("segment-%d.data", mergedID))
	if err := os.Rename(tmp, mergedPath); err != nil {
		return err
	}

	// Lock and close old segments
	oldSegments := db.segments
	for _, s := range oldSegments {
		s.mu.Lock()
		s.file.Close()
		os.Remove(s.path)
		s.mu.Unlock()
	}

	// Create new merged segment
	sf, err := os.Open(mergedPath)
	if err != nil {
		return err
	}
	st, _ := sf.Stat()
	db.segments = []*segment{{file: sf, id: mergedID, size: st.Size(), path: mergedPath}}

	// Rebuild index
	db.index = make(map[string]position)
	if err := db.scanSegment(db.segments[0]); err != nil {
		return err
	}
	if err := db.scanSegment(db.active); err != nil {
		return err
	}
	return nil
}

func (db *DB) copyUnique(src *segment, dst *os.File, seen map[string]struct{}) error {
	src.file.Seek(0, io.SeekStart)
	r := bufio.NewReader(src.file)
	for {
		var e entry
		_, err := e.DecodeFromReader(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if _, ok := seen[e.key]; ok {
			continue
		}
		seen[e.key] = struct{}{}
		if _, err := dst.Write(e.Encode()); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) segIdx(id int) int {
	for i, s := range db.segments {
		if s.id == id {
			return i
		}
	}
	return -1
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.value)
	buf := make([]byte, 4+4+kl+vl)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(kl))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(vl))
	copy(buf[8:8+kl], e.key)
	copy(buf[8+kl:], e.value)
	return buf
}

func (e *entry) Decode(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid data: too short for header: %d bytes", len(data))
	}

	kl := binary.LittleEndian.Uint32(data[0:4])
	vl := binary.LittleEndian.Uint32(data[4:8])
	if int(8+kl+vl) > len(data) {
		return fmt.Errorf("invalid data: expected %d bytes, got %d", 8+kl+vl, len(data))
	}

	e.key = string(data[8 : 8+kl])
	e.value = string(data[8+kl : 8+kl+vl])
	return nil
}

func (e *entry) DecodeFromReader(r io.Reader) (int, error) {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, err
	}
	kl := binary.LittleEndian.Uint32(hdr[0:4])
	vl := binary.LittleEndian.Uint32(hdr[4:8])
	buf := make([]byte, kl+vl)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	e.key = string(buf[:kl])
	e.value = string(buf[kl:])
	return 8 + int(kl) + int(vl), nil
}

// PutInt64 зберігає int64 як string
func (db *DB) PutInt64(key string, value int64) error {
	return db.Put(key, strconv.FormatInt(value, 10))
}

// GetInt64 повертає значення як int64
func (db *DB) GetInt64(key string) (int64, error) {
	strVal, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strVal, 10, 64)
}
