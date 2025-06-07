package datastore

import "os"

// segment – один файл-лог (заморожений або активний).
type segment struct {
	file *os.File
	id   int
	size int64
	path string
}
