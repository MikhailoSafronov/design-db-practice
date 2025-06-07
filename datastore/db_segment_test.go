package datastore

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time" // ← NEW
)

func TestSegmentRotationAndMerge(t *testing.T) {
	t.Setenv("SEG_MAX", "16384")
	time.Sleep(time.Millisecond) // ← NEW: гарантуємо, що env підхопився

	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	for i := 0; i < n; i++ {
		if err := db.Put(fmt.Sprintf("k%04d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}

	// читаємо всі ключі
	for i := 0; i < n; i++ {
		if _, err := db.Get(fmt.Sprintf("k%04d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// запускаємо merge без очікування тікера
	_ = db.merge()

	files, _ := os.ReadDir(dir)
	segCnt := 0
	for _, f := range files {
		if segRE.MatchString(f.Name()) ||
			strings.HasPrefix(f.Name(), "snapshot-") {
			segCnt++
		}
	}
	if segCnt != 1 {
		t.Fatalf("expected 1 frozen segment, got %d", segCnt)
	}

	// ще раз перевіряємо читання
	for i := 0; i < n; i++ {
		if _, err := db.Get(fmt.Sprintf("k%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()
}
