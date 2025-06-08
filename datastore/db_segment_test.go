package datastore

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestSegmentRotationAndMerge(t *testing.T) {
	dir := "test_segment_rotation"
	defer os.RemoveAll(dir)

	t.Setenv("SEG_MAX", "50")

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Записуємо дані для ротації
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := strings.Repeat("v", 20)
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Перевіряємо заморожені сегменти
	if len(db.segments) == 0 {
		t.Errorf("expected at least 1 frozen segment, got %d", len(db.segments))
	}

	// Виконуємо злиття
	if err := db.merge(); err != nil {
		t.Fatal(err)
	}

	// Перевіряємо кількість сегментів після злиття
	if len(db.segments) != 1 {
		t.Errorf("expected 1 segment after merge, got %d", len(db.segments))
	}

	// Перевіряємо цілісність даних
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		expected := strings.Repeat("v", 20)
		if value != expected {
			t.Errorf("expected %s, got %s", expected, value)
		}
	}
}
