package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const defaultOutFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64
type fileIndex map[string]int

const TenMegabytes = 10 * 1024 * 1024

type Db struct {
	out         *os.File
	outPath     string
	outOffset   int64
	fileCounter int

	index       hashIndex
	fileMap     fileIndex
	maxFileSize int64
}

func NewDb(dir string, maxFileSize ...int64) (*Db, error) {
	var size int64
	if len(maxFileSize) > 0 {
		size = maxFileSize[0]
	} else {
		size = TenMegabytes // default size
	}

	fileCounter, err := getFileCounter(dir)
	if err != nil {
		return nil, err
	}

	outputPath := filepath.Join(dir, defaultOutFileName+"-"+strconv.Itoa(fileCounter))
	fmt.Println("Path ", outputPath)

	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath:     outputPath,
		out:         f,
		index:       make(hashIndex),
		fileMap:     make(fileIndex),
		maxFileSize: size,
		fileCounter: 0,
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	input, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer input.Close()

	outInfo, err := input.Stat()
	size1 := outInfo.Size()
	fmt.Println("Size", size1)

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.index[e.key] = db.outOffset
			db.outOffset += int64(n)
			db.fileMap[e.key] = 0 /// we need to know file index here
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	fileNumber, ok := db.fileMap[key]
	if !ok {
		return "", ErrNotFound
	}

	filePath := filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(fileNumber))
	fmt.Println("Path:", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	fileInfo, err := db.out.Stat()
	if err != nil {
		return err
	}

	// Check if the file size is exceeding the limit
	if fileInfo.Size() > db.maxFileSize {
		// Close the current file
		db.out.Close()

		// Open a new file
		db.fileCounter++
		db.outPath = filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(db.fileCounter))
		db.out, err = os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}
		db.outOffset = 0
	}
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.fileMap[key] = db.fileCounter
		db.outOffset += int64(n)
	}
	return err
}

func getFileCounter(dir string) (int, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	// Initialize the fileCounter to 0
	fileCounter := 0
	// Find the file with the highest counter in the directory
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), defaultOutFileName+"-") {
			// Extract the counter from the filename
			counter, err := strconv.Atoi(strings.TrimPrefix(f.Name(), defaultOutFileName+"-"))
			if err != nil {
				return 0, err
			}
			// Update the fileCounter if this file has a higher counter
			if counter > fileCounter {
				fileCounter = counter
			}
		}
	}
	return fileCounter, nil
}
