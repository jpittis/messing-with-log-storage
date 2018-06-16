package simpledb

import (
	"fmt"
	"io"
	"os"
)

const BlockSize = 4096

type Block interface {
	ID() uint32
	Serialize() []byte
}

func SerializeBlock(file *os.File, block Block) error {
	data := block.Serialize()
	pos := block.ID() * BlockSize
	_, err := file.Seek(int64(pos), io.SeekEnd)
	if err != nil {
		return err
	}
	n, err := file.Write(data)
	if err != nil {
		return err
	}
	if n != BlockSize {
		return fmt.Errorf("wrote less than block size %d", n)
	}
	return nil
}

// Ensure that records fit within BlockSize.
type RecordBlock struct {
	id      uint32
	records []Record
}

func (b *RecordBlock) ID() uint32 {
	return b.id
}

func (b *RecordBlock) Serialize() []byte {
	// Too much copying than there should be but it gets the job done.
	data := make([]byte, 0, BlockSize)
	for _, r := range b.records {
		rdata := append(serialize(string(r.Key)), serialize(string(r.Value))...)
		data = append(data, rdata...)
	}
	return data
}

type BlockedFileStore struct {
	file *os.File
}

func NewBlockedFileStore(filename string) (*BlockedFileStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	return &BlockedFileStore{file: file}, nil
}
