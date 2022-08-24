package main

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
	"math/rand"

	log "github.com/sirupsen/logrus"
	xxh3 "github.com/zeebo/xxh3"

	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func NewSpreadHash(src_ff *FlexFile, concurrency int, nodes int, nodeID int, bytes_per_thread uint64, progress bool) (*NFSInfo, error) {

	if bytes_per_thread == 0 {
		bytes_per_thread = getBytesPerThread(src_ff.size, nodes, concurrency)
	}

	needed_threads_per_node := getThreadCount(src_ff.size, uint64(nodes), bytes_per_thread)

	if  needed_threads_per_node < uint64(concurrency) {
		//File is too small for this concurrency, we are reducing it.
		concurrency = int(needed_threads_per_node)
		log.Infof("Thread count reduced to %d because of a small file. ", concurrency)
	} else if needed_threads_per_node > uint64(concurrency) {
		log.Warnf("Thread count is increased to %d in order to hash entire file. ", concurrency)
		concurrency = int(needed_threads_per_node)
	}

	
	nfsHash := &NFSInfo{
		src_ff: src_ff, 
		concurrency: concurrency, 
	    hashes: make([][]byte, concurrency),
		thread_bytes: make([]uint64, concurrency),
	    progress: progress }
	
	if !nfsHash.src_ff.exists {
		log.Fatalf("Error: source fle %s doesn't exist", nfsHash.src_ff.file_name)
	}


	nfsHash.sizeMB = bytes_per_thread
	nfsHash.nodeSize =  bytes_per_thread * uint64(concurrency)
	nfsHash.nodeOffset = uint64(nodeID) * nfsHash.nodeSize
	
	return nfsHash, nil
}

func (n *NFSInfo) SpreadHash() (float64, []byte) {

	atomic.StoreInt32(&n.atm_finished, 0)
	atomic.StoreUint64(&n.atm_counter_bytes_written, 0)
	
	var p *mpb.Progress
	if n.progress {
		p = mpb.New(
			mpb.WithWaitGroup(&n.wg),
			mpb.WithWidth(60),
			mpb.WithRefreshRate(1000*time.Millisecond),
		)
	}
	
	start := time.Now()

	offset := n.nodeOffset

	for i := 0; i < n.concurrency ; i++ {
	
		// check to see if we would hit end of the file before even starting.
		//if (offset) > n.src_ff.size {
		//	break
		//}

		// also we can't exceed the end of the file.
		max_bytes_to_read := n.sizeMB
		if max_bytes_to_read + offset  > n.src_ff.size  {
			max_bytes_to_read = n.src_ff.size - offset
		}

		var bar *mpb.Bar
		if n.progress{
			name := fmt.Sprintf("Thread#%d:", i)
			bar = p.AddBar(int64(max_bytes_to_read),
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
		}
		n.wg.Add(1)
		go n.hashOneFileChunk(offset, max_bytes_to_read, i, bar)
		offset += n.sizeMB
	}
	n.wg.Wait()
	
	hasher := xxh3.New()

	if n.hashes[0] == nil {
		log.Panic("First Hash invalid")
	}

	for  i :=  0 ; i < len(n.hashes); i++ {
		if n.hashes[i] == nil {
			log.Debugf("Thread %d hash: %x  offset: %d  bytes: %d",
		                  i+1, n.hashes[i],n.nodeOffset + uint64(i) * n.sizeMB, n.thread_bytes[i])
		} else {
			hasher.Write(n.hashes[i])
		}
	}
	hashValue :=hasher.Sum([]byte{})
	

	elapsed := time.Since(start)
	total_mb_bytes := atomic.LoadUint64(&n.atm_counter_bytes_read) / ( 1024 * 1024 )
	
	//fmt.Printf("Hash Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_mb_bytes)
	
	return float64(total_mb_bytes) / (float64(elapsed.Seconds())  ) , hashValue
}

func (n *NFSInfo) hashOneFileChunk(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

    defer n.wg.Done()

	//sleep time between threads
	//Avoids slamming server 
	time.Sleep(time.Duration(threadID) * 2 * time.Millisecond)

	max_bytes_to_read := num_bytes
	
	var f_src ReadWriteSeekerCloser = nil
	
	var err error = nil

	// Open the source file.
	f_src, err = n.src_ff.Open()
	if err != nil {
		// Retry once,
		sleepfor := 100 + rand.Intn(50)
		log.Warnf("Thread %d will retry in %d miliseconds", threadID, sleepfor)
		time.Sleep(time.Duration(sleepfor) * time.Millisecond)
		f_src, err = n.src_ff.Open()
		if err != nil{
			log.Fatalf("Error opening source file: %s", n.src_ff.file_full_path)
		}
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

		if thread_bytes_read == max_bytes_to_read {
			break
		} else if thread_bytes_read > max_bytes_to_read {
			log.Fatal("More bytes read than expected.")
		}
	}
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	n.thread_bytes[threadID] = thread_bytes_read
	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
}