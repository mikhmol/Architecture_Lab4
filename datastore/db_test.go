package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		//fmt.Println("Temporary Directory Path:", dir)
		os.RemoveAll(dir)
	}()

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, defaultOutFileName) + "-0")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		t.Log("Run put/get test")
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

}

func TestDb_Put_Merge(t *testing.T) {
	// Create temporary directory for test files
	dir, err := ioutil.TempDir("", "test-db-rotation")
	if err != nil {
		t.Fatalf("Could not create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a new Db with max file size of 1 byte
	db, err := NewDb(dir, 1)
	if err != nil {
		t.Fatalf("Could not create DB: %v", err)
	}

	// Put several items in the DB
	keys := []string{"key1", "key2", "key3", "key4"}
	for _, key := range keys {
		err := db.Put(key, "value")
		if err != nil {
			t.Fatalf("Could not put item: %v", err)
		}
	}

	// Wait for the merge operation to complete
	db.wg.Wait()

	// Check if multiple files are created
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatalf("Could not read directory: %v", err)
	}
	// for _, file := range files {
	// 	fmt.Println(file.Name())
	// }
	//expectedNumFiles := len(keys) // Since max file size is 1 byte, we expect one file per key
	expectedNumFiles := 2
	if len(files) != expectedNumFiles {
		t.Errorf("Expected %d files, got %d", expectedNumFiles, len(files))
	}

	// Check if all keys are present in the merged DB (index consistency)
	for _, key := range keys {
		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("Could not get item: %v", err)
		}
		if value != "value" {
			t.Errorf("Expected value 'value', got '%s'", value)
		}
	}
}
