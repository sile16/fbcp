package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	xxh3 "github.com/zeebo/xxh3"

	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func NewSpreadHash(src_ff *FlexFile, concurrency int, nodes int, nodeID int ) (*NFSInfo, error) {

	nfsHash := &NFSInfo{
		src_ff: src_ff, 
		concurrency: concurrency, 
	    hashes: make([][]byte, concurrency) }
	
	if !nfsHash.src_ff.exists {
		log.Fatalf("Error: source fle %s doesn't exist", nfsHash.src_ff.file_name)
	}

	// min each thread will get minimum of 32 MB of data
	min_thread_size  := uint64(32) * 1024 * 1024

	bytes_per_thread := nfsHash.src_ff.size / uint64( nodes * concurrency)
	remainder_per_thread :=  bytes_per_thread % min_thread_size
	bytes_per_thread += min_thread_size - remainder_per_thread

	if bytes_per_thread < min_thread_size{
		bytes_per_thread = min_thread_size
	}

	nfsHash.sizeMB = bytes_per_thread
	nfsHash.nodeSize =  bytes_per_thread * uint64(concurrency)
	nfsHash.nodeOffset = uint64(nodeID) * nfsHash.nodeSize
	
	return nfsHash, nil
}

func (n *NFSInfo) SpreadHash() (float64, []byte) {

	atomic.StoreInt32(&n.atm_finished, 0)
	atomic.StoreUint64(&n.atm_counter_bytes_written, 0)
	
	p := mpb.New(
		mpb.WithWaitGroup(&n.wg),
	    mpb.WithWidth(60),
		mpb.WithRefreshRate(1000*time.Millisecond),
	)
	
	start := time.Now()

	offset := n.nodeOffset

	for i := 0; i < n.concurrency && offset < n.src_ff.size; i++ {
	
		// check to see if we would hit end of the file before even starting.
		if (offset) > n.src_ff.size {
			break
		}

		// also we can't exceed the end of the file.
		max_bytes_to_read := n.sizeMB
		if max_bytes_to_read + offset  > n.src_ff.size  {
			max_bytes_to_read = n.src_ff.size - offset
		}

		name := fmt.Sprintf("Thread#%d:", i)

		bar := p.AddBar(int64(max_bytes_to_read),
					mpb.PrependDecorators(
						decor.Name(name),
						decor.CountersKibiByte("% .1f / % .1f"),
					),
					mpb.AppendDecorators(
						decor.EwmaETA(decor.ET_STYLE_GO, 90),
						decor.Name(" ] "),
						decor.EwmaSpeed(decor.UnitKiB, "% .1f", 60),
					),
				)
		n.wg.Add(1)
		go n.hashOneFileChunk(offset, max_bytes_to_read, i, bar)
		offset += n.sizeMB
	}
	n.wg.Wait()
	
	hasher := xxh3.New()
	for  i :=  0 ; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue :=hasher.Sum([]byte{})
	

	elapsed := time.Since(start)
	total_mb_bytes := atomic.LoadUint64(&n.atm_counter_bytes_written) / ( 1024 * 1024 )
	
	fmt.Printf("Write Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_mb_bytes)
	
	return float64(total_mb_bytes) / (float64(elapsed.Seconds())  ) , hashValue
}

func (n *NFSInfo) hashOneFileChunk(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

	defer n.wg.Done()
	max_bytes_to_read := num_bytes
	
	var f_src ReadWriteSeekerCloser = nil
	
	var err error = nil

	// Open the source file.
	f_src, err = n.src_ff.Open()
	if err != nil {
		fmt.Print("Error opening source file.")
		return
	}
	defer f_src.Close()
	

	srcBuf := make([]byte,1*1024*1024)

	//hasher := md5.New()
	hasher := xxh3.New()
	
	thread_bytes_read := uint64(0)

	f_src.Seek(int64(offset), io.SeekStart)

	for {
		if atomic.LoadInt32(&n.atm_finished) == 1 {
			break
		}
		
		remaining_bytes := max_bytes_to_read - thread_bytes_read
		bytes_to_read :=  uint64(len(srcBuf))

		if bytes_to_read > remaining_bytes {
			bytes_to_read = remaining_bytes
		}
		n_bytes, err := f_src.Read(srcBuf[0:bytes_to_read])
		thread_bytes_read += uint64(n_bytes)

		if err != nil {
			if !(err == io.EOF && n_bytes == int(bytes_to_read)) {
				fmt.Printf("Thread %d Error Read Error\n",threadID)
				fmt.Printf("%s\n", err)
				return
			}
		}
		
		hasher.Write(srcBuf[0:n_bytes])
	}
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
}