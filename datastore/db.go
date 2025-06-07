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

/*─────────────────── константи / глобальні ───────────────────*/

const (
	activeName      = "current-data"   // ім’я активного сегмента
	defaultMaxBytes = 10 * 1024 * 1024 // 10 МБ у продакшені
)

var (
	ErrNotFound    = fmt.Errorf("record does not exist")
	segRE          = regexp.MustCompile(`^segment-(\d+)\.data$`)
	MaxSegmentSize = int64(defaultMaxBytes) // актуалізується під час Put()
)

/*───────────────────── структура DB ─────────────────────────*/

type DB struct {
	dir      string
	segments []*segment          // segment-N | snapshot-X
	active   *segment            // id = –1
	index    map[string]position // key → {segID, offset}

	mu   sync.RWMutex
	quit chan struct{}
}

/*────────────────────── Open / Recover ──────────────────────*/

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		dir:   dir,
		index: make(map[string]position),
		quit:  make(chan struct{}),
	}

	if err := db.loadSegments(); err != nil {
		return nil, err
	}
	if err := db.recover(); err != nil {
		return nil, err
	}
	go db.compactor()
	return db, nil
}

func (db *DB) loadSegments() error {
	ents, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}

	// зібрати id вже заморожених
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

	// заморожені
	for _, id := range ids {
		p := filepath.Join(db.dir, fmt.Sprintf("segment-%d.data", id))
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		st, _ := f.Stat()
		db.segments = append(db.segments, &segment{file: f, id: id, size: st.Size(), path: p})
	}

	// активний
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

/*────────────────────────── API ──────────────────────────────*/

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
		s = db.segments[db.segIdx(pos.segID)]
	}
	db.mu.RUnlock()

	// читаємо size-header
	hdr := make([]byte, 4)
	if _, err := s.file.ReadAt(hdr, pos.offset); err != nil {
		return "", err
	}
	size := int64(binary.LittleEndian.Uint32(hdr))

	// читаємо увесь запис
	buf := make([]byte, size)
	copy(buf, hdr)
	if _, err := s.file.ReadAt(buf[4:], pos.offset+4); err != nil {
		return "", err
	}

	var e entry
	e.Decode(buf)
	return e.value, nil
}

func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// динамічно оновлюємо поріг (корисно в тестах із t.Setenv)
	if v := os.Getenv("SEG_MAX"); v != "" {
		if n, _ := strconv.ParseInt(v, 10, 64); n > 0 {
			MaxSegmentSize = n
		}
	}

	e := entry{key: key, value: value}
	data := e.Encode()

	offset := db.active.size
	if _, err := db.active.file.Write(data); err != nil {
		return err
	}
	db.index[key] = position{segID: -1, offset: offset}
	db.active.size += int64(len(data))

	if db.active.size >= MaxSegmentSize {
		return db.rotateActive()
	}
	return nil
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
	var first error
	for _, s := range append(db.segments, db.active) {
		if err := s.file.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

/*──────────────────────── ротація ───────────────────────────*/

func (db *DB) rotateActive() error {
	db.active.file.Sync()
	db.active.file.Close()

	nextID := 0
	if len(db.segments) > 0 {
		nextID = db.segments[len(db.segments)-1].id + 1
	}
	frozen := filepath.Join(db.dir, fmt.Sprintf("segment-%d.data", nextID))
	if err := os.Rename(db.active.path, frozen); err != nil {
		return err
	}
	fz, err := os.Open(frozen)
	if err != nil {
		return err
	}
	db.segments = append(db.segments, &segment{file: fz, id: nextID, size: db.active.size, path: frozen})

	// оновлюємо індекс: старі записи тепер у segment-N
	for k, pos := range db.index {
		if pos.segID == -1 {
			pos.segID = nextID
			db.index[k] = pos
		}
	}

	// створюємо новий активний
	newPath := filepath.Join(db.dir, activeName)
	af, err := os.OpenFile(newPath, os.O_CREATE|os.O_APPEND|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	db.active = &segment{file: af, id: -1, size: 0, path: newPath}
	return nil
}

/*─────────────────── compactor / merge ──────────────────────*/

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

func (db *DB) merge() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.segments) < 2 {
		return nil
	}

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

	snap := filepath.Join(db.dir, fmt.Sprintf("snapshot-%d.data", time.Now().UnixNano()))
	if err := os.Rename(tmp, snap); err != nil {
		return err
	}

	for _, s := range db.segments {
		s.file.Close()
		os.Remove(s.path)
	}
	db.segments = nil
	sf, _ := os.Open(snap)
	st, _ := sf.Stat()
	db.segments = []*segment{{file: sf, id: 0, size: st.Size(), path: snap}}

	db.index = make(map[string]position)
	db.scanSegment(db.segments[0])
	db.scanSegment(db.active)
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

/*──────────────────── helper ───────────────────────────────*/

func (db *DB) segIdx(id int) int {
	for i, s := range db.segments {
		if s.id == id {
			return i
		}
	}
	return -1
}
