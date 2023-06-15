package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
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
	out        *os.File
	outPath    string
	outOffset  int64
	outSegment int

	// keep indexes: key offset in file and key segment
	index     hashIndex
	fileIndex fileIndex

	maxFileSize int64
}

func NewDb(dir string, maxFileSize ...int64) (*Db, error) {
	var size int64
	if len(maxFileSize) > 0 {
		size = maxFileSize[0]
	} else {
		size = TenMegabytes // default size
	}

	maxSegmentIndex, err := getMaxSegmentNumber(dir)
	if err != nil {
		return nil, err
	}

	outputPath := filepath.Join(dir, defaultOutFileName+"-"+strconv.Itoa(maxSegmentIndex))
	fmt.Println("Path ", outputPath)

	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath:     outputPath,
		out:         f,
		index:       make(hashIndex),
		fileIndex:   make(fileIndex),
		maxFileSize: size,
		outSegment:  maxSegmentIndex,
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

// shall recover data indexes for all avaliable segments
func (db *Db) recover() error {
	files, err := ioutil.ReadDir(filepath.Dir(db.outPath))
	if err != nil {
		return err
	}

	// sort the files in ascending order,
	// current-data-1, current-data-2 etc
	// it is important to maintain c orrect indexes
	// later added data override old ones
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), defaultOutFileName+"-") {
			continue
		}

		filePath := filepath.Join(filepath.Dir(db.outPath), file.Name())

		// extract segment index from the filename
		segment, err := strconv.Atoi(strings.TrimPrefix(file.Name(), defaultOutFileName+"-"))
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
				db.fileIndex[e.key] = segment
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
	segment, ok := db.fileIndex[key]
	if !ok {
		return "", ErrNotFound
	}

	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	filePath := filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(segment))
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
		db.outSegment++
		db.outPath = filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(db.outSegment))
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
		db.fileIndex[key] = db.outSegment
		db.outOffset += int64(n)
	}
	return err
}

// scam directory to get max existing segment file and return its index
func getMaxSegmentNumber(dir string) (int, error) {
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
