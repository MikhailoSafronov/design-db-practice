package datastore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"empty", "", ""},
		{"simple", "key", "value"},
		{"special chars", "hello@world", "test!@#$%^&*()"},
		{"long values", strings.Repeat("a", 1000), strings.Repeat("b", 2000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := entry{key: tt.key, value: tt.value}
			encoded := e.Encode()

			// Перевірка розміру
			expectedSize := 8 + len(tt.key) + len(tt.value)
			if len(encoded) != expectedSize {
				t.Errorf("Expected size %d, got %d", expectedSize, len(encoded))
			}

			// Перевірка заголовка
			kl := binary.LittleEndian.Uint32(encoded[0:4])
			vl := binary.LittleEndian.Uint32(encoded[4:8])
			if int(kl) != len(tt.key) {
				t.Errorf("Expected key length %d, got %d", len(tt.key), kl)
			}
			if int(vl) != len(tt.value) {
				t.Errorf("Expected value length %d, got %d", len(tt.value), vl)
			}

			// Перевірка даних
			keyData := string(encoded[8 : 8+kl])
			valueData := string(encoded[8+kl : 8+kl+vl])
			if keyData != tt.key {
				t.Errorf("Expected key %q, got %q", tt.key, keyData)
			}
			if valueData != tt.value {
				t.Errorf("Expected value %q, got %q", tt.value, valueData)
			}
		})
	}
}

func TestEntry_Decode(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"empty", "", ""},
		{"simple", "key", "value"},
		{"unicode", "ключ", "значення"},
		{"long values", strings.Repeat("a", 1000), strings.Repeat("b", 2000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Створюємо запис та кодуємо його
			e := entry{key: tt.key, value: tt.value}
			encoded := e.Encode()

			// Декодуємо
			var decoded entry
			if err := decoded.Decode(encoded); err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Порівнюємо результати
			if decoded.key != tt.key {
				t.Errorf("Expected key %q, got %q", tt.key, decoded.key)
			}
			if decoded.value != tt.value {
				t.Errorf("Expected value %q, got %q", tt.value, decoded.value)
			}
		})
	}
}

func TestEntry_DecodeFromReader(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"empty", "", ""},
		{"simple", "key", "value"},
		{"special chars", "test@example.com", "password!123"},
		{"long values", strings.Repeat("x", 500), strings.Repeat("y", 1500)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Створюємо запис та кодуємо його
			e := entry{key: tt.key, value: tt.value}
			data := e.Encode()

			// Створюємо читача
			reader := bufio.NewReader(bytes.NewReader(data))
			var decoded entry
			n, err := decoded.DecodeFromReader(reader)
			if err != nil {
				t.Fatalf("DecodeFromReader failed: %v", err)
			}

			// Перевіряємо кількість прочитаних байтів
			if n != len(data) {
				t.Errorf("Expected %d bytes read, got %d", len(data), n)
			}

			// Порівнюємо результати
			if decoded.key != tt.key {
				t.Errorf("Expected key %q, got %q", tt.key, decoded.key)
			}
			if decoded.value != tt.value {
				t.Errorf("Expected value %q, got %q", tt.value, decoded.value)
			}
		})
	}
}

func TestEntry_Decode_Errors(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "nil data",
			data: nil,
		},
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "short header (4 bytes)",
			data: []byte{1, 0, 0, 0},
		},
		{
			name: "short header (7 bytes)",
			data: []byte{1, 0, 0, 0, 2, 0, 0},
		},
		{
			name: "invalid key length",
			data: func() []byte {
				buf := make([]byte, 12)
				binary.LittleEndian.PutUint32(buf[0:4], 1000) // kl = 1000
				binary.LittleEndian.PutUint32(buf[4:8], 10)   // vl = 10
				return buf
			}(),
		},
		{
			name: "invalid value length",
			data: func() []byte {
				buf := make([]byte, 12)
				binary.LittleEndian.PutUint32(buf[0:4], 4)    // kl = 4
				binary.LittleEndian.PutUint32(buf[4:8], 2000) // vl = 2000
				copy(buf[8:], []byte("test"))
				return buf
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e entry
			err := e.Decode(tc.data)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tc.name)
			} else {
				t.Logf("Correct error: %v", err)
			}
		})
	}
}

func TestEntry_DecodeFromReader_Errors(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "short header (4 bytes)",
			data: []byte{1, 0, 0, 0},
		},
		{
			name: "short header (7 bytes)",
			data: []byte{1, 0, 0, 0, 2, 0, 0},
		},
		{
			name: "incomplete body",
			data: func() []byte {
				buf := make([]byte, 12)
				binary.LittleEndian.PutUint32(buf[0:4], 10) // kl = 10
				binary.LittleEndian.PutUint32(buf[4:8], 20) // vl = 20
				// Тіло відсутнє
				return buf
			}(),
		},
		{
			name: "partial body",
			data: func() []byte {
				buf := make([]byte, 15)
				binary.LittleEndian.PutUint32(buf[0:4], 5) // kl = 5
				binary.LittleEndian.PutUint32(buf[4:8], 5) // vl = 5
				copy(buf[8:], []byte("hello"))             // Тільки ключ
				return buf
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bufio.NewReader(bytes.NewReader(tc.data))
			var e entry
			_, err := e.DecodeFromReader(reader)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tc.name)
			} else {
				t.Logf("Correct error: %v", err)
			}
		})
	}
}

func TestEntry_Consistency(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{"", ""},
		{"key", "value"},
		{"hello world", "привіт світ"},
		{strings.Repeat("a", 100), strings.Repeat("b", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			// Перевірка прямого та зворотнього перетворення
			e1 := entry{key: tt.key, value: tt.value}
			encoded := e1.Encode()

			var e2 entry
			if err := e2.Decode(encoded); err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if e1 != e2 {
				t.Errorf("Direct decode mismatch: %v != %v", e1, e2)
			}

			// Перевірка через читача
			reader := bufio.NewReader(bytes.NewReader(encoded))
			var e3 entry
			if _, err := e3.DecodeFromReader(reader); err != nil {
				t.Fatalf("DecodeFromReader failed: %v", err)
			}

			if e1 != e3 {
				t.Errorf("Reader decode mismatch: %v != %v", e1, e3)
			}
		})
	}
}
