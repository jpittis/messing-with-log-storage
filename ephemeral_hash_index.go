package simpledb

import "errors"

var ErrIndexNotFound = errors.New("index not found")

type EphemeralHashIndex struct {
	lookup map[Key]Position
}

func NewEphemeralHashIndex() *EphemeralHashIndex {
	return &EphemeralHashIndex{
		lookup: map[Key]Position{},
	}
}

func (i *EphemeralHashIndex) Read(key Key) (Position, error) {
	pos, ok := i.lookup[key]
	if !ok {
		return 0, ErrIndexNotFound
	}
	return pos, nil

}
func (i *EphemeralHashIndex) Write(key Key, pos Position) error {
	i.lookup[key] = pos
	return nil
}

func (i *EphemeralHashIndex) Delete(key Key) error {
	delete(i.lookup, key)
	return nil
}
