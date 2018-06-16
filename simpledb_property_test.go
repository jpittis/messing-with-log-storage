package simpledb

import (
	"io"
	"math/rand"
	"strconv"
	"testing"
)

const MaxStoreSize = 4096 * 100

type RunEvent func(*SimpleDB, Record) error

func TestPropertyFileStoreWithEphemeralHashIndex(t *testing.T) {
	store, err := NewTempFileStore()
	if err != nil {
		t.Fatal(err)
	}
	index := NewEphemeralHashIndex()
	db := NewSimpleDB(store, index)
	RunPropertyTest(t, db)
}

// Run a number of randomly generated events on both the system under test (sit) and the
// model. Raise all hell if they don't have the same behaviour.
func RunPropertyTest(t *testing.T, sit *SimpleDB) {
	model := NewSimpleDB(NewMemoryStore(MaxStoreSize), NewEphemeralHashIndex())

	events := []RunEvent{
		func(db *SimpleDB, record Record) error {
			return db.Write(record.Key, record.Value)
		},
		func(db *SimpleDB, record Record) error {
			_, err := db.Read(record.Key)
			return err
		},
		func(db *SimpleDB, record Record) error {
			return db.Delete(record.Key)
		},
	}

	for i := 0; i < MaxStoreSize; i++ {
		record := genRecord()
		n := rand.Intn(3)
		err1 := events[n](model, record)
		err2 := events[n](sit, record)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("model and sit did not behave the same way with errors: %s and %s", err1, err2)
		}
	}

	modelDump, err := model.store.Dump()
	if err != nil {
		t.Fatal(err)
	}
	sitDump, err := sit.store.Dump()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if len(modelDump) != len(sitDump) {
		t.Fatalf("expected sit and model to be the same size %d != %d",
			len(modelDump), len(sitDump))
	}
	for i, d := range modelDump {
		if sitDump[i].Key != d.Key || sitDump[i].Value != d.Value {
			t.Fatalf("expected record %d to have %+v == %+v.", i, sitDump[i], d)
		}
	}
}

func genRecord() Record {
	key := Key("key" + strconv.Itoa(rand.Intn(1024)))       // Generate a key in the space of 1024.
	value := Value("value" + strconv.Itoa(rand.Intn(2048))) // Generate a value in the space of 2048.
	return Record{key, value}
}
