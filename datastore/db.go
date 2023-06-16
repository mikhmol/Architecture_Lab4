package datastore

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

const (
	defaultOutFileName = "data-segment"
	TenMegabytes       = 10 * 1024 * 1024
	workerPoolSize     = 20 // Change this value to control the maximum number of concurrent file descriptors
)

var ErrNotFound = fmt.Errorf("record does not exist")
var goroutineID int64

type hashIndex map[string]int64

// keep segment indexes per key
type fileIndex map[string]int

type Db struct {
	out        *os.File
	outPath    string
	outOffset  int64
	outSegment int

	// indexes:
	index     hashIndex // key -> offset
	fileIndex fileIndex // key -> segment

	maxFileSize int64

	wg sync.WaitGroup // for unit tests
	mu sync.RWMutex   // synchronize access to the file index

	workerPool *semaphore.Weighted
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
		workerPool:  semaphore.NewWeighted(workerPoolSize),
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

		// reset offset for a new file
		db.outOffset = 0

		var buf [bufSize]byte
		in := bufio.NewReaderSize(input, bufSize)

		// read data from file and decode
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
				db.index[e.key] = db.outOffset // out offset relevant for the last segment only
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

	db.mu.RLock()         // Lock for reading
	defer db.mu.RUnlock() // Unlock after operation

	segment, ok := db.fileIndex[key]
	if !ok {
		return "", ErrNotFound
	}

	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	// Wait until a worker is available
	if err := db.workerPool.Acquire(context.Background(), 1); err != nil {
		// This should never happen under normal circumstances
		return "", fmt.Errorf("acquire worker: %w", err)
	}
	defer db.workerPool.Release(1)

	filePath := filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-"+strconv.Itoa(segment))
	fmt.Println("Get segment:", filepath.Base(filePath))
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

	db.mu.Lock()         // Lock for writing
	defer db.mu.Unlock() // Unlock after operation

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
		db.outOffset = 0 // reset offset for a new file

		// Start a goroutine to merge segments to delete not actual data
		db.wg.Add(1) // increment the WaitGroup counter before starting the goroutine
		go func(id int64) {
			defer db.wg.Done() // decrement the counter when the function completes
			fmt.Printf("Goroutine %d is merging segment files\n", id)
			db.mergeSegmentFiles(id)
		}(atomic.AddInt64(&goroutineID, 1)) // generate unique ID and pass it as an argument
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

// scan directory to get max existing segment file and return its index
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

// merge files, lock indexes when merge
func (db *Db) mergeSegmentFiles(id int64) error {
	fmt.Printf("Goroutine %d started merging\n", id)

	// Lock the mutex for writing, this will block all Get/Put operations
	db.mu.Lock()
	defer db.mu.Unlock()

	files, err := ioutil.ReadDir(filepath.Dir(db.outPath))
	if err != nil {
		return err
	}

	if len(files) <= 2 {
		fmt.Printf("Goroutine %d skip merging\n", id)
		return nil // nothing to merge
	}

	fmt.Printf("Goroutine %d merge files in %s\n", id, filepath.Dir(db.outPath))

	fileNames := GetFilesToMerge(files, db.outSegment)

	mergedData := make(map[string]entry)

	for _, fileName := range fileNames {

		filePath := filepath.Join(filepath.Dir(db.outPath), fileName)
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
				mergedData[e.key] = e
			}
		}
		input.Close()
	}

	// Remove segment files
	for index, fileName := range fileNames {
		if index == 0 {
			continue // Skip the first file, it will be used for merging
		}
		filePath := filepath.Join(filepath.Dir(db.outPath), fileName)
		err = os.Remove(filePath)
		if err != nil {
			fmt.Println("Error removing file:", err)
			return err
		}
		fmt.Println("Removed file:", fileName)
	}

	outputPath := filepath.Join(filepath.Dir(db.outPath), defaultOutFileName+"-0")
	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	fmt.Println("Merged file:", filepath.Base(outputPath))
	defer file.Close()

	//var mergedIndex = make(hashIndex)
	var entryOffset int64 = 0 // keep offset in a file
	for _, e := range mergedData {
		n, err := file.Write(e.Encode())
		fmt.Println("Add", e) // trace what is added
		if err == nil {
			//mergedIndex[e.key] = entryOffset

			// find key in DB file index
			if segment, ok := db.fileIndex[e.key]; ok {
				// if key is not in the out segment, update offset and segment
				if segment != db.outSegment {
					db.index[e.key] = entryOffset
					db.fileIndex[e.key] = 0
				}
			}
			entryOffset += int64(n)
		}
	}

	fmt.Printf("Goroutine %d finished merging\n", id)

	return nil

}

func GetFilesToMerge(files []fs.FileInfo, outSegment int) []string {
	fileNames := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() ||
			!strings.HasPrefix(file.Name(), defaultOutFileName+"-") ||
			strings.HasPrefix(file.Name(), defaultOutFileName+"-"+strconv.Itoa(outSegment)) {
			continue
		}
		fileNames = append(fileNames, file.Name())
	}
	sort.Strings(fileNames)
	return fileNames

}
