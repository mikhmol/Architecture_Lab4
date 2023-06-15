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

// keep segment indexes per key
type fileIndex map[string]int

const TenMegabytes = 10 * 1024 * 1024

type Db struct {
	out          *os.File
	outPath      string
	outOffset    int64
	segmentIndex int

	index         hashIndex
	segmmentIndex fileIndex
	maxFileSize   int64
}

func NewDb(dir string, maxFileSize ...int64) (*Db, error) {
	var size int64
	if len(maxFileSize) > 0 {
		size = maxFileSize[0]
	} else {
		size = TenMegabytes // default size
	}

	fileCounter, err := getMaxSegmentIndex(dir)
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
		outPath:       outputPath,
		out:           f,
		index:         make(hashIndex),
		segmmentIndex: make(fileIndex),
		maxFileSize:   size,
		segmentIndex:  0,
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

// shall process all files in directory
func (db *Db) recover() error {
	files, err := ioutil.ReadDir(filepath.Dir(db.outPath))
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), defaultOutFileName+"-") {
			continue
		}

		filePath := filepath.Join(filepath.Dir(db.outPath), file.Name())

		// extract segment index from the filename
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file.Name(), defaultOutFileName+"-"))
		if err != nil {
			return err
		}

		// Open the file
		input, err := os.Open(filePath)
		if err != nil {
			return err
		}

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
					break
				}
			} else if err != nil {
				input.Close()
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
					input.Close()
					return fmt.Errorf("corrupted file")
				}

				var e entry
				e.Decode(data)
				db.index[e.key] = db.outOffset
				db.outOffset += int64(n)
				db.segmmentIndex[e.key] = segmentIndex
			}
		}

		// Close the file, maybe left last one opened after recovering?
		input.Close()
	}

	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	segmentIndex, ok := db.segmmentIndex[key]
	if !ok {
		return "", ErrNotFound
	}

	filePath := filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(segmentIndex))
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

		// Open a new segment file
		db.segmentIndex++
		db.outPath = filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(db.segmentIndex))
		db.out, err = os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}
		db.outOffset = 0

		// Start a goroutine to merge segments to delete not actual data
		go db.mergeSegmentFiles()

	}

	e := entry{
		key:   key,
		value: value,
	}

	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.segmmentIndex[key] = db.segmentIndex
		db.outOffset += int64(n)

	}
	return err
}

// scam directory to get max existing segment file and return its index
func getMaxSegmentIndex(dir string) (int, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	maxIndex := 0
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), defaultOutFileName+"-") {
			index, err := strconv.Atoi(strings.TrimPrefix(f.Name(), defaultOutFileName+"-"))
			if err != nil {
				return 0, err
			}
			if index > maxIndex {
				maxIndex = index
			}
		}
	}
	return maxIndex, nil
}

// Merge all old files into one
func (db *Db) mergeSegmentFiles() {
	fmt.Println("Merge segment files", db.outPath)
	//TODO: implement merge
}
