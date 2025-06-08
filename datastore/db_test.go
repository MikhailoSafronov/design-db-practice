package datastore

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestConcurrentAccess(t *testing.T) {
	dir := "test_db"
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Використовуємо унікальні ключі для кожного потоку
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Put failed: %v", err)
				return
			}

			gotValue, err := db.Get(key)
			if err != nil {
				t.Errorf("Get failed: %v", err)
				return
			}

			if gotValue != value {
				t.Errorf("For key %s: expected %s, got %s", key, value, gotValue)
			}
		}(i)
	}
	wg.Wait()
}

func TestSize(t *testing.T) {
	dir := "test_db_size"
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test Size method
	size, err := db.Size()
	if err != nil {
		t.Fatalf("Size() error: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// Add some data
	err = db.Put("test", "value")
	if err != nil {
		t.Fatal(err)
	}

	size, err = db.Size()
	if err != nil {
		t.Fatalf("Size() error: %v", err)
	}
	if size <= 0 {
		t.Errorf("Expected size > 0, got %d", size)
	}
}
