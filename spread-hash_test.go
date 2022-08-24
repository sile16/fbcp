package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	log "github.com/sirupsen/logrus"
)

const MB uint64 = 1024 * 1024

var hash_tests = []struct {
	name string
	size uint64
	threads int
	expected string
}{

	// 16 Threads
	{"Empty File", 0, 16, "69da940740c0040b"},
	{"1 Byte File",1 ,16,  "70b30bdcfe088d72"},
	{"3 Byte File", 3, 16,  "91d71c71c09fbce0"},
	{"Min size minus 1 byte", min_thread_size - 1, 16,  "1d342169a679aa0d"},
	{"Min size",min_thread_size,16,   "0194b09b08323189"},
	
	{"Min size + 1 byte", min_thread_size + 1, 16,  "b1a2b18ef19c9425"},
	{"64MB", 64 * MB, 16,  "e199642d6a8faa44"},
	{"64MB +1", 64 * MB + 1, 16,  "5b99e57350ebedf3"},
	{"512MB",512 * MB,16,  "9081b6722086479c"},
	{"2G", 2048 * MB,16,   "b034caa2fc3a22f3"},
	{"2G -1", 2048 * MB - 1, 16,"60ddb8540582118e"},

	// 1 Threads
	{"Empty File", 		0, 1, "69da940740c0040b"},
	{"1 Byte File",		1 ,1,  "70b30bdcfe088d72"},
	{"3 Byte File", 	3, 1,  "91d71c71c09fbce0"},
	{"Min size minus 1 byte", min_thread_size - 1, 1,  "1d342169a679aa0d"},
	{"Min size",min_thread_size,1,   "0194b09b08323189"},
	
	{"Min size + 1 byte", min_thread_size + 1, 1,  "5dca84a61c7cefd0"},
	{"64MB", 64 * MB,1,  "f113a1b015d7c178"},
	{"64MB +1", 64 * MB + 1,1,  "0cc5a528bcb8f3a7"},
	{"512MB",512 * MB,1,  "1c880af72d4f961b"},
	{"2G", 2048 * MB,1,   "a9d152f0af30f080"},
	{"2G -1", 2048 * MB - 1, 1,"537de2e155f4b9e5"},

	
}

func create_tmp_file(t *testing.T, file_size uint64, buf []byte)  {

	file_path := filepath.Join("tempdir", strconv.FormatUint(file_size, 10))
	f, err := os.Create(file_path)
    if err != nil{
		t.Log("Temp file already exists.")
	}
    defer f.Close()

	
	for written := uint64(0) ; written < file_size ; {
		bytes_to_write := uint64(len(buf))
		if written + bytes_to_write > file_size {
			bytes_to_write = file_size - written
		}
		n_bytes, err := f.Write(buf[:bytes_to_write])
		if err != nil{
			panic(err)
		}

		written += uint64(n_bytes)
	}	
}


// You can use testing.T, if you want to test the code without benchmarking
func setupSuite(tb *testing.T) {
	//log.Println("setup suite")
	srcBuf := make([]byte, 1024*1024)

	//Deterministic random buffer so same files are generated each time.
	rand.Seed(89239342)
	rand.Read(srcBuf)

	//setup
	os.Mkdir("tempdir", os.ModePerm)
	for _, tc := range hash_tests {
		create_tmp_file(tb, tc.size, srcBuf)
	}
}

func TestSpreadHash(tb *testing.T) {
	setupSuite(tb)
	log.SetLevel(log.DebugLevel)

	for _, tc := range hash_tests {
		name := tc.name + "__" + strconv.FormatInt(int64(tc.threads),10) + " Threads"
		tb.Run(name, func(t *testing.T) {

			file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))

			src_ff, _ := NewFlexFile(file_path)
			nfshash, _ := NewSpreadHash(src_ff, tc.threads, 1, 0,0, false)
			_, hash := nfshash.SpreadHash()
	
			hash_string := fmt.Sprintf("%x",hash) 

			if hash_string != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, hash_string)
			}
		})
	}

 
}