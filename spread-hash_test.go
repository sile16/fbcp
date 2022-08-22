package main

import (
	"math/rand"
	"testing"
	"os"
	"strconv"
)

const MB uint64 = 1024 * 1024

var tests = []struct {
	size uint64
	want string
}{
	{0, ""},
	{1, ""},
	{3, ""},
	{min_thread_size - 1, "[]}"},
	{min_thread_size, "[]"},
	{min_thread_size + 1, "[]"},
	{64 * MB,"[]"},
	{64 * MB + 1,"[]"},
	{512 * MB,"[]"},
	{2048 * MB, "[]"},
	{2048 * MB - 1, "[]"},
}

func create_tmp_file(file_size uint64, buf []byte)  {

	f, err := os.Create(strconv.FormatUint(file_size, 10))
    if err != nil{
		panic(err)
	}
    defer f.Close()

	for written := uint64(0) ; written < file_size ; {
		bytes_to_write := uint64(len(buf))
		if written + bytes_to_write > file_size {
			bytes_to_write = file_size - written
		}
		n_bytes, err := f.Write(buf)
		if err != nil{
			panic(err)
		}

		written += uint64(n_bytes)
	}	
}

func str(file_size uint64) {
	panic("unimplemented")
}

// You can use testing.T, if you want to test the code without benchmarking
func setupSuite(tb *testing.TB) {
	//log.Println("setup suite")
	srcBuf := make([]byte, 1024*1024)

	//Deterministic random buffer so same files are generated each time.
	rand.Seed(89239342)
	rand.Read(srcBuf)

	tb.TempDir()

}

func TestSpread(tb *testing.TB) {
	teardownSuite := setupSuite(t)
	defer teardownSuite(t)

}