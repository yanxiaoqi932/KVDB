package kvdb

import (
	"kvdb/file"
	"testing"
)

func TestOpen(t *testing.T) {
	ent := &file.Entry{
		Key:   []byte("key 1"),
		Value: []byte("value 1"),
		Type:  file.EntryMeta,
	}

	t.Run("Open", func(t *testing.T) {
		db, err := Open()
		if err != nil {
			t.Errorf("Open() Error:%v\n", err)
		}
		err = db.WriteEntry(String, ent, 0)
		if err != nil {
			t.Errorf("WriteEntry() Error:%v\n", err)
		}
		value, err := db.ReadEntry(String, []byte("key 1"), 0)
		if err != nil {
			t.Errorf("ReadEntry() Error:%v\n", err)
		}
		if string(value) != "value 1" {
			t.Errorf("ReadEntry() wrong")
		}
	})
}
