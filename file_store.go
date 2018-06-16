package simpledb

import (
	"encoding/binary"
	"io"
	"os"
)

type FileStore struct {
	file *os.File
}

func NewFileStore(filename string) (*FileStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	return &FileStore{file: file}, nil
}

func (s *FileStore) Write(key Key, val Value) (Position, error) {
	data := append(serialize(string(key)), serialize(string(val))...)
	pos, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	_, err = s.file.Write(data)
	return Position(uint32(pos)), err
}

func (s *FileStore) Read(pos Position) (Value, error) {
	_, err := s.file.Seek(int64(pos), io.SeekStart)
	if err != nil {
		return "", err
	}
	_, err = deserialize(s.file)
	if err != nil {
		return "", err
	}
	foundVal, err := deserialize(s.file)
	if err != nil {
		return "", err
	}
	return Value(foundVal), err
}

func (s *FileStore) Delete(key Key) error {
	_, err := s.Write(key, Value(""))
	return err
}

func (s *FileStore) Dump() ([]Record, error) {
	records := make([]Record, 0)
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return records, err
	}
	for {
		record, err := nextRecord(s.file)
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
}

func nextRecord(r io.Reader) (Record, error) {
	var record Record
	foundKey, err := deserialize(r)
	if err != nil {
		return record, err
	}
	foundValue, err := deserialize(r)
	if err != nil {
		return record, err
	}
	record.Key = Key(foundKey)
	record.Value = Value(foundValue)
	return record, nil
}

func serialize(str string) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(str)))
	return append(buf[:], []byte(str)...)
}

func deserialize(r io.Reader) (string, error) {
	var lenbuf [8]byte
	_, err := r.Read(lenbuf[:])
	if err != nil {
		return "", err
	}
	length := binary.LittleEndian.Uint64(lenbuf[:])
	valbuf := make([]byte, length)
	_, err = r.Read(valbuf)
	return string(valbuf), err
}
