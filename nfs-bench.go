package main

import (
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	xxh3 "github.com/zeebo/xxh3"
)

// Will return either zeros or random bytes.
type VirtualSourceReader struct {
	zeros bool
	size int
	pos int

	buf []byte
}

//create a new virtualsourcereader
func NewVirtualSourceReader(size int, zeros bool) *VirtualSourceReader {
	vsr := &VirtualSourceReader{size: size, zeros: zeros, pos: 0}
	vsr.buf = make([]byte, size)
	if !zeros {
		rand.Read(vsr.buf)
	}
	return vsr
}


func (vsr *VirtualSourceReader) Read(p []byte) (n int, err error) {
	bytes_left := len(p)
	bytes_read := 0

	for bytes_left > 0 {
		single_read_size := bytes_left
		if single_read_size + vsr.pos > len(vsr.buf) {
			single_read_size = len(vsr.buf)
		}
		
		//copy from buffer to p
		n := copy(p[bytes_read:bytes_read+single_read_size], vsr.buf[vsr.pos:vsr.pos+single_read_size])
		bytes_read += n
		bytes_left -= n
		vsr.pos += n
		if vsr.pos == len(vsr.buf) {
			vsr.pos = 0
		}
	}
	return int(bytes_read), nil	
}

//create virtualsource reader seek to offset
func (vsr *VirtualSourceReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		vsr.pos = int(offset)
	case io.SeekCurrent:
		vsr.pos += int(offset)
	case io.SeekEnd:
		vsr.pos = len(vsr.buf) + int(offset)
	}
	vsr.pos = vsr.pos % len(vsr.buf)
	return int64(vsr.pos), nil
}


func NewNFSBench(dst_ff *FlexFile, concurrency int, nodes int, nodeID int, sizeMB uint64, hash bool, zeros bool) (*NFSInfo, error) {
	// set to debug logging
	//util.DefaultLogger.SetDebug(true)

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

	f, err := n.dst_ff.Open()
	if err != nil {
		log.Errorf("Error opening destination file. %s"	, err)
		return
	}
	defer f.Close()


	bytes_written := uint64(0)

	vsr := NewVirtualSourceReader(int(512*1024), n.zeros)
	vsr_limited := io.LimitReader(vsr, int64(n.sizeMB*1024*1024))


	_, err = f.Seek(int64(n.nodeOffset+offset), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking in destination file. %s"	, err)
		return 
	}
	n_bytes, err := f.ReadFrom(vsr_limited)
	if err != nil {
		log.Errorf("Error writing to destination file. %s"	, err)
		return
	}

	if n_bytes != int64(n.sizeMB*1024*1024) {
		log.Errorf("Error writing to destination file. %d bytes written, expected %d bytes"	, n_bytes, n.sizeMB*1024*1024)
	}
	
	//todo; move this outside the timer
	hasher := xxh3.New()
	if n.hash {
		vsr.Seek(0, io.SeekStart)
		hash_vsr_limited := io.LimitReader(vsr, int64(n.sizeMB*1024*1024))
		io.CopyBuffer(hasher, hash_vsr_limited, nil)
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, bytes_written)
}

func (n *NFSInfo) ReadTest() (float64, []byte) {
	fmt.Println("Starting Read Test")

	atomic.StoreUint64(&n.atm_counter_bytes_read, 0)

	start := time.Now()

	offset := uint64(0)
	for i := 0; i < n.concurrency; i++ {
		fmt.Printf("Starting thread %d/n", i)
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
	fmt.Print("starting readOneFileChunk`")

	p := make([]byte, 512*1024)
	byte_counter := uint64(0)

	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	hasher := xxh3.New()

	f.Seek(int64(n.nodeOffset+offset), io.SeekStart)
	log.Infof("Thread: %d Reading from offset %d", threadID, n.nodeOffset+offset)
	for {
		// Read 1 MB
		start_index := 0
		for {
			chunk := len(p)
			if start_index+chunk > len(p) {
				chunk = len(p) - start_index
			}
			n_bytes, err := f.Read(p[start_index : chunk+start_index])
			log.Debug("Read %d bytes", n_bytes)
			start_index += n_bytes

			if start_index == len(p) {
				break
			}

			if err == io.EOF {
				fmt.Printf("Thread %d Warning: Unexpected End of File! \n", threadID)
				break
			}
		}

		// Calculate Hash
		if n.hash {
			hasher.Write(p)
		}

		byte_counter += uint64(len(p))

		if byte_counter == n.sizeMB*1024*1024 {
			break
		}

		if byte_counter == n.sizeMB*1024*1024/2 {
			fmt.Printf("Thread Read %d - 50%%\n", threadID)
		}
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	fmt.Printf("Thread Read %d - Done!!!!! \n", threadID)
	atomic.AddUint64(&n.atm_counter_bytes_read, byte_counter)
}
