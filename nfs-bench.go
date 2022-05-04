package main

import (
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"
	"crypto/md5"
)

func NewNFSBench(dst_ff *FlexFile, concurrency int,  nodes int, nodeID int, sizeMB uint64, verify bool ) (*NFSInfo, error) {
	nodeOffset := uint64(nodeID) * uint64(concurrency) * sizeMB * 1024 * 1024

	nfsBench := &NFSInfo{ 
		concurrency: concurrency, filesWritten: 0, dst_ff: dst_ff,
	    hashes: make([][]byte, concurrency), sizeMB: sizeMB, nodeOffset: nodeOffset, verify: verify}
	
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

	hasher := md5.New()
	for  i :=  0 ; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue :=hasher.Sum([]byte{})

	elapsed := time.Since(start)
	total_bytes := atomic.LoadUint64(&n.atm_counter_bytes_written) / ( 1024 * 1024 )
	
	fmt.Printf("Write Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_bytes)
	fmt.Printf("Written Data Hash: %x\n", hashValue )

	return float64(total_bytes) / (float64(elapsed.Seconds())  ) , hashValue
}

func (n *NFSInfo) writeOneFileChunk(offset uint64, threadID int) {
	defer n.wg.Done()

	srcBuf := make([]byte,16 * 1024*1024)

	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	hasher := md5.New()
	
	var bytes_written uint64
	bytes_written = 0

	rand.Read(srcBuf)
	f.Seek(int64(n.nodeOffset + offset), io.SeekStart)
	for {
		n_bytes, _ := f.Write(srcBuf)
		if n_bytes != len(srcBuf) {
			fmt.Printf("Thread %d Warning: Not all bytes written!", threadID)
		}
		if n.verify {
			 hasher.Write(srcBuf)
		}
		bytes_written += uint64(n_bytes)

		if bytes_written >= n.sizeMB * 1024 * 1024{
			break
		}
		
		//if i == n.sizeMB / 2 {
		//	fmt.Printf("Thread Write %d - Chunk 50%% \n", threadID)
		//}
	}
	fmt.Printf("Thread Write %d - Done !!!!!! \n", threadID)
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, bytes_written)
}

func (n *NFSInfo) ReadTest() ( float64, []byte ) {

	atomic.StoreUint64(&n.atm_counter_bytes_read, 0)

	start := time.Now()

	offset := uint64(0)
	for i := 0; i < n.concurrency; i++ {
		n.wg.Add(1)
		go n.readOneFileChunk(offset, i)
		offset += n.sizeMB * 1024 * 1024
	}

	n.wg.Wait()
	
	hasher := md5.New()
	for  i :=  0 ; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue :=hasher.Sum([]byte{})

	elapsed := time.Since(start)
	total_bytes := atomic.LoadUint64(&n.atm_counter_bytes_read) / ( 1024 * 1024 )
	
	fmt.Printf("Read Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_bytes)
	fmt.Printf("Read Data Hash: %x\n", hashValue)

	return float64(total_bytes) / float64(elapsed.Seconds()) , hashValue
}

func (n *NFSInfo) readOneFileChunk(offset uint64, threadID int) {
	defer n.wg.Done()

	hasher := md5.New()
	p := make([]byte, 16*1024*1024)
	byte_counter := uint64(0)

	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	f.Seek(int64(n.nodeOffset + offset), io.SeekStart)
	half := false

	for {
		n_bytes, err := f.Read(p)
		if n.verify {
			hasher.Write(p)
		}
		byte_counter += uint64(n_bytes)
		
		if byte_counter == n.sizeMB * 1024*1024 {
			break
		}
		
		if  byte_counter > n.sizeMB*1024*1024 {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}

		if err == io.EOF {
			fmt.Printf("Thread %d Warning: Unexpected End of File! \n", threadID)
			break
		}

		if !half && byte_counter >= n.sizeMB*1024*1024/2 {
			fmt.Printf("Thread Read %d - 50%%\n", threadID)
			half = true
		}
	}
	n.hashes[threadID] = hasher.Sum([]byte{})
 	fmt.Printf("Thread Read %d - Done!!!!! \n", threadID)
	atomic.AddUint64(&n.atm_counter_bytes_read, byte_counter)
}