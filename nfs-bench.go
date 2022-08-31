package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	xxh3 "github.com/zeebo/xxh3"
)

func NewNFSBench(dst_ff *FlexFile, concurrency int, nodes int, nodeID int, sizeMB uint64, hash bool, zeros bool) (*NFSInfo, error) {
	nodeOffset := uint64(nodeID) * uint64(concurrency) * sizeMB * 1024 * 1024

	nfsBench := &NFSInfo{
		concurrency: concurrency, filesWritten: 0, dst_ff: dst_ff,
		hashes: make([][]byte, concurrency), sizeMB: sizeMB, nodeOffset: nodeOffset,
		hash: hash, zeros: zeros}

	total_file_size := int64(nodes * concurrency * int(sizeMB) * 1024 * 1024)
	if !nfsBench.dst_ff.exists || nfsBench.dst_ff.size != uint64(total_file_size) {
		//truncate
		dst_ff.Truncate(total_file_size)
	}

	return nfsBench, nil
}

func (n *NFSInfo) WriteTest() (float64, []byte) {
	atomic.StoreUint64(&n.atm_counter_bytes_written, 0)

	start := time.Now()
	offset := uint64(0)

	for i := 0; i < n.concurrency; i++ {
		n.wg.Add(1)
		go n.writeOneFileChunk(offset, i)
		offset += n.sizeMB * 1024 * 1024
	}
	n.wg.Wait()
	elapsed := time.Since(start)

	hasher := xxh3.New()
	for i := 0; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue := hasher.Sum([]byte{})

	total_bytes := atomic.LoadUint64(&n.atm_counter_bytes_written) / (1024 * 1024)

	fmt.Printf("Write Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_bytes)
	fmt.Printf("Written Data Hash: %x\n  ", hashValue)

	return float64(total_bytes) / (float64(elapsed.Seconds())), hashValue
}

func (n *NFSInfo) writeOneFileChunk(offset uint64, threadID int) {
	defer n.wg.Done()

	srcBuf := make([]byte, 1024*1024)
	for i := range srcBuf {
		srcBuf[i] = 0
	}
	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	hasher := xxh3.New()

	var bytes_written uint64
	bytes_written = 0

	if !n.zeros {
		rand.Read(srcBuf)
	}

	f.Seek(int64(n.nodeOffset+offset), io.SeekStart)
	for {
		n_bytes, err := f.Write(srcBuf)
		if err != nil {
			fmt.Printf("Error writing to file. %s", err)
			break
		}
		if n_bytes != len(srcBuf) {
			fmt.Printf("Thread %d Warning: Not all bytes written!", threadID)
		}
		if n_bytes < len(srcBuf) {
			fmt.Print("Not all bytes written")
		}
		bytes_written += uint64(n_bytes)

		if bytes_written == n.sizeMB*1024*1024 {
			break
		} else if bytes_written > n.sizeMB*1024*1024 {
			fmt.Print("Wrote more bytes than expected.")
			break
		}
	}
	fmt.Printf("Thread Write %d - Done !!!!!! \n", threadID)

	// this relies on a 1MB buffer
	if n.hash {
		for i := 0; uint64(i) < n.sizeMB; i++ {
			hasher.Write(srcBuf)
		}
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, bytes_written)
}

func (n *NFSInfo) ReadTest() (float64, []byte) {

	atomic.StoreUint64(&n.atm_counter_bytes_read, 0)

	start := time.Now()

	offset := uint64(0)
	for i := 0; i < n.concurrency; i++ {
		n.wg.Add(1)
		go n.readOneFileChunk(offset, i)
		offset += n.sizeMB * 1024 * 1024
	}

	n.wg.Wait()

	hasher := xxh3.New()

	for i := 0; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue := hasher.Sum([]byte{})

	elapsed := time.Since(start)
	total_bytes := atomic.LoadUint64(&n.atm_counter_bytes_read) / (1024 * 1024)

	return float64(total_bytes) / float64(elapsed.Seconds()), hashValue
}

func (n *NFSInfo) readOneFileChunk(offset uint64, threadID int) {
	defer n.wg.Done()

	hasher := xxh3.New()
	var hash_buff []byte
	if n.hash {
		hash_buff = make([]byte, 1024*1024)
	}
	p := make([]byte, 1024*1024)
	byte_counter := uint64(0)

	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	f.Seek(int64(n.nodeOffset+offset), io.SeekStart)
	for {

		// Read 1 MB
		start_index := 0
		for {
			chunk := len(p)
			if start_index+chunk > len(p) {
				chunk = len(p) - start_index
			}
			n_bytes, err := f.Read(p[start_index : chunk+start_index])
			start_index += n_bytes
			if start_index == len(p) {
				break
			}

			if err == io.EOF {
				fmt.Printf("Thread %d Warning: Unexpected End of File! \n", threadID)
				break
			}
		}

		// Verify the 1 MB Chunk
		if n.hash {
			if byte_counter == 0 {
				// we read the first 1MB chunk of the file, and that pattern is used over and over
				copy(hash_buff, p)
			} else {
				// now we just do a byte compare to validate the data but compute the hashes later.
				if !bytes.Equal(p, hash_buff) {
					fmt.Printf("Data Compare Failed.\n")
				}
			}
		}

		byte_counter += uint64(len(p))

		if byte_counter == n.sizeMB*1024*1024 {
			break
		}

		if byte_counter > n.sizeMB*1024*1024 {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}

		if byte_counter == n.sizeMB*1024*1024/2 {
			fmt.Printf("Thread Read %d - 50%%\n", threadID)
		}
	}

	// this relies on a 1MB buffer
	if n.hash {
		for i := 0; uint64(i) < n.sizeMB; i++ {
			hasher.Write(hash_buff)
		}
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	fmt.Printf("Thread Read %d - Done!!!!! \n", threadID)
	atomic.AddUint64(&n.atm_counter_bytes_read, byte_counter)
}
