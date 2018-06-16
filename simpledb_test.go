package simpledb

import (
	"io"
	"io/ioutil"
	"testing"
)

func TestFileStoreWithEphemeralHashIndex(t *testing.T) {
	store, err := NewTempFileStore()
	if err != nil {
		t.Fatal(err)
	}
	index := NewEphemeralHashIndex()
	db := NewSimpleDB(store, index)
	SimpleSimpleDBTest(t, db)
}

func SimpleSimpleDBTest(t *testing.T, db *SimpleDB) {
	myKey := Key("foo")
	myVal := Value("bar")
	myKey2 := Key("baz")
	myVal2 := Value("lol")

	records, err := db.store.Dump()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("Expected records to be empty but was %v", records)
	}

	val, err := db.Read(myKey)
	if err == nil {
		t.Fatalf("Expected to not find value %s.", val)
	}

	err = db.Write(myKey, myVal)
	if err != nil {
		t.Fatalf("Failed to write to database with %s", err)
	}

	err = db.Write(myKey2, myVal2)
	if err != nil {
		t.Fatalf("Failed to write to database with %s", err)
	}

	val, err = db.Read(myKey)
	if err != nil {
		t.Fatalf("Failed to read from database with %s.", err)
	}
	if val != myVal {
		t.Fatalf("Expected val %s == %s.", myVal, val)
	}

	val, err = db.Read(myKey2)
	if err != nil {
		t.Fatalf("Failed to read from database with %s.", err)
	}
	if val != myVal2 {
		t.Fatalf("Expected val %s == %s.", myVal2, val)
	}

	err = db.Write(myKey, myVal2)
	if err != nil {
		t.Fatalf("Failed to write to database with %s", err)
	}

	val, err = db.Read(myKey)
	if err != nil {
		t.Fatalf("Failed to read from database with %s.", err)
	}
	if val != myVal2 {
		t.Fatalf("Expected val %s == %s.", myVal2, val)
	}

	err = db.Delete(myKey)
	if err != nil {
		t.Fatalf("Failed to delete key with %s.", err)
	}

	val, err = db.Read(myKey)
	if err == nil {
		t.Fatalf("Expected to not find value %s.", val)
	}

	val, err = db.Read(myKey2)
	if err != nil {
		t.Fatalf("Failed to read from database with %s.", err)
	}
	if val != myVal2 {
		t.Fatalf("Expected val %s == %s.", myVal2, val)
	}

	records, err = db.store.Dump()
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("Expected records to be 4 but was %v", records)
	}

	expectedDump := []Record{
		Record{Key: "foo", Value: "bar"},
		Record{Key: "baz", Value: "lol"},
		Record{Key: "foo", Value: "lol"},
		Record{Key: "foo", Value: ""},
	}
	for i, d := range expectedDump {
		if records[i].Key != d.Key || records[i].Value != d.Value {
			t.Fatalf("Expected %v == %v.", records[i], d)
		}
	}
}

func NewTempFileStore() (*FileStore, error) {
	tmpfile, err := ioutil.TempFile("", "simpledb_test")
	if err != nil {
		return nil, err
	}
	tmpfile.Close()
	return NewFileStore(tmpfile.Name())
}
