package simpledb

type Key string
type Value string
type Position uint32

type Record struct {
	Key   Key
	Value Value
}

type Store interface {
	Write(Key, Value) (Position, error)
	Read(Position) (Value, error)
	Delete(Key) error
	Dump() ([]Record, error)
}

type Index interface {
	Read(Key) (Position, error)
	Write(Key, Position) error
	Delete(Key) error
}

type SimpleDB struct {
	store Store
	index Index
}

func NewSimpleDB(store Store, index Index) *SimpleDB {
	return &SimpleDB{
		store: store,
		index: index,
	}
}

func (db *SimpleDB) Write(key Key, value Value) error {
	pos, err := db.store.Write(key, value)
	if err != nil {
		return err
	}
	return db.index.Write(key, pos)
}

func (db *SimpleDB) Read(key Key) (Value, error) {
	pos, err := db.index.Read(key)
	if err != nil {
		return "", err
	}
	return db.store.Read(pos)
}

func (db *SimpleDB) Delete(key Key) error {
	err := db.index.Delete(key)
	if err != nil {
		return err
	}
	return db.store.Delete(key)
}
