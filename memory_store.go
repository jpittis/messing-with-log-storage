package simpledb

// The simplest in memory store I could think of which I can use as the model for property
// testing the more complex stores.
type MemoryStore struct {
	nextPosition Position
	store        []Record
}

func NewMemoryStore(size int) *MemoryStore {
	return &MemoryStore{
		nextPosition: Position(0),
		store:        make([]Record, size),
	}
}

func (s *MemoryStore) Write(key Key, val Value) (Position, error) {
	pos := s.getNextPosition()
	s.store[pos] = Record{key, val}
	return pos, nil
}

func (s *MemoryStore) Read(pos Position) (Value, error) {
	return s.store[pos].Value, nil
}
func (s *MemoryStore) Delete(key Key) error {
	_, err := s.Write(key, Value(""))
	return err
}

func (s *MemoryStore) Dump() ([]Record, error) {
	return s.store[0:s.nextPosition], nil
}

func (s *MemoryStore) getNextPosition() Position {
	next := s.nextPosition
	s.nextPosition++
	return next
}
