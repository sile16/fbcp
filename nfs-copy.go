package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func NewNFSCopy(src_ff *FlexFile, dst_ff *FlexFile, concurrency int, nodes int, nodeID int , verify bool, copyv2 bool) (*NFSInfo, error) {

	nfsNFSCopy := &NFSInfo{
		src_ff: src_ff, dst_ff: dst_ff, 
		concurrency: concurrency, filesWritten: 0,
	    hashes: make([][]byte, concurrency), verify: verify, copyv2: copyv2}
	
	if !nfsNFSCopy.src_ff.exists {
		fmt.Printf("Error: source fle %s doesn't exist", nfsNFSCopy.src_ff.file_name)
		os.Exit(1)
	}

	//todo; make it work on directories, right now we will be explicit
	if nfsNFSCopy.dst_ff.is_directory{
		log.Fatal("Target path is a directory")									
	}

	if !nfsNFSCopy.dst_ff.exists || nfsNFSCopy.dst_ff.size != nfsNFSCopy.src_ff.size {
		//truncate
		dst_ff.Truncate(int64(src_ff.size))
	}

	// min each thread will get minimum of 32 MB of data
	
	min_thread_size  := uint64(32) * 1024 * 1024

	bytes_per_thread := nfsNFSCopy.src_ff.size / uint64( nodes * concurrency)
	remainder_per_thread :=  bytes_per_thread % min_thread_size
	bytes_per_thread += min_thread_size - remainder_per_thread

	if bytes_per_thread < min_thread_size{
		bytes_per_thread = min_thread_size
	}

	nfsNFSCopy.sizeMB = bytes_per_thread
	nfsNFSCopy.nodeSize =  bytes_per_thread * uint64(concurrency)
	nfsNFSCopy.nodeOffset = uint64(nodeID) * nfsNFSCopy.nodeSize
	
	return nfsNFSCopy, nil
}

func (n *NFSInfo) SpreadCopy() (float64, []byte) {

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
		if n.copyv2 {
			go n.copyOneFileChunkv2(offset, max_bytes_to_read, i, bar)
		} else {
			go n.copyOneFileChunk(offset, max_bytes_to_read, i, bar)
		}
		
		offset += n.sizeMB
	}
	n.wg.Wait()
	
	hasher := md5.New()
	for  i :=  0 ; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue :=hasher.Sum([]byte{})
	

	elapsed := time.Since(start)
	total_bytes := atomic.LoadUint64(&n.atm_counter_bytes_written) / ( 1024 * 1024 )
	
	//fmt.Printf("Written Data Hash: %x\n", hashValue )
	fmt.Printf("Write Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_bytes)
	
	return float64(total_bytes) / (float64(elapsed.Seconds())  ) , hashValue
}

func (n *NFSInfo) copyOneFileChunk(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

	defer n.wg.Done()
	max_bytes_to_read := num_bytes
	
	var f_src ReadWriteSeekerCloser = nil
	var f_dst ReadWriteSeekerCloser = nil
	
	var err error = nil

	// Open the source file.

	f_src, err = n.src_ff.Open()
	if err != nil {
		fmt.Print("Error opening source file.")
		return
	}
	defer f_src.Close()

	// Open the Dest File
	f_dst, err = n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f_dst.Close()
	

	srcBuf := make([]byte,1*1024*1024)

	hasher := md5.New()
	
	thread_bytes_written := uint64(0)
	thread_bytes_read := uint64(0)

	f_src.Seek(int64(offset), io.SeekStart)
	f_dst.Seek(int64(offset), io.SeekStart)
	

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
		if n.verify {
			hasher.Write(srcBuf[0:n_bytes])
		}
		
		n_bytes_written_this_chunk := uint64(0)
		for {
			n_bytes_written, err := f_dst.Write(srcBuf[n_bytes_written_this_chunk:n_bytes])

			if err != nil {
				if err == io.EOF {
					log.Fatalf("Thread %d Warning: Unexpected End of File! \n", threadID)
					break
				}
				log.Fatalf("Thread %d Error: write error! %s\n", threadID, err)
				break
			}
			n_bytes_written_this_chunk += uint64(n_bytes_written)
			bar.IncrBy(n_bytes_written)
			if n_bytes_written_this_chunk == uint64(n_bytes) {
				break
			}
		}
		thread_bytes_written += n_bytes_written_this_chunk

		if thread_bytes_written == max_bytes_to_read {
			break
		}
		
		if  thread_bytes_written > max_bytes_to_read {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}
	}
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, thread_bytes_written)
}

func (n *NFSInfo) copyOneFileChunkv2(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

	defer n.wg.Done()
	
	var f_src ReadWriteSeekerCloser = nil
	var f_dst ReadWriteSeekerCloser = nil
	
	var err error = nil

	// Open the source file.

	f_src, err = n.src_ff.Open()
	if err != nil {
		fmt.Print("Error opening source file.")
		return
	}
	defer f_src.Close()

	// Open the Dest File
	f_dst, err = n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f_dst.Close()
	
	hasher := md5.New()
	
	thread_bytes_written := uint64(0)
	thread_bytes_read := uint64(0)

	f_src.Seek(int64(offset), io.SeekStart)
	f_dst.Seek(int64(offset), io.SeekStart)
	
	bytes_written, err := io.CopyN(f_dst, f_src, int64(num_bytes))
	if err != nil {
		log.Fatalf("Only Copied %d bytes in thread %d, at offset %d , Error: %s", bytes_written, threadID, offset, err)
	}
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, thread_bytes_written)
}
