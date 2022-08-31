package main

import (
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	xxh3 "github.com/zeebo/xxh3"

	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func NewNFSCopy(src_ff *FlexFile, dst_ff *FlexFile, concurrency int, nodes int, nodeID int,
	bytes_per_thread uint64, hash bool, copyv2 bool, progress bool) (*NFSInfo, error) {

	if bytes_per_thread == 0 {
		bytes_per_thread = getBytesPerThread(src_ff.size, nodes, concurrency)
	}

	needed_threads_per_node := getThreadCount(src_ff.size, uint64(nodes), bytes_per_thread)

	if needed_threads_per_node < uint64(concurrency) {
		//File is too small for this concurrency, we are reducing it.
		concurrency = int(needed_threads_per_node)
		log.Infof("Thread count reduced to %d because of a small file. ", concurrency)
	} else if needed_threads_per_node > uint64(concurrency) {
		log.Warnf("Thread count is increased to %d in order to hash entire file. ", concurrency)
		concurrency = int(needed_threads_per_node)
	}

	nfsNFSCopy := &NFSInfo{
		src_ff: src_ff, dst_ff: dst_ff,
		concurrency: concurrency, filesWritten: 0,
		hashes:       make([][]byte, concurrency),
		thread_bytes: make([]uint64, concurrency),
		hash:         hash, copyv2: copyv2}

	if !nfsNFSCopy.src_ff.exists {
		log.Fatalf("Error: source fle %s doesn't exist", nfsNFSCopy.src_ff.file_name)
	}

	//todo; make it work on directories, right now we will be explicit
	if nfsNFSCopy.dst_ff.is_directory {
		log.Fatal("Target path is a directory")
	}

	if !nfsNFSCopy.dst_ff.exists || nfsNFSCopy.dst_ff.size != nfsNFSCopy.src_ff.size {
		//truncate
		dst_ff.Truncate(int64(src_ff.size))
	}

	// min each thread will get minimum of 16 MB of data
	// this also handles small files gracefully.
	if bytes_per_thread < min_thread_size {
		bytes_per_thread = min_thread_size
	}

	nfsNFSCopy.sizeMB = bytes_per_thread
	nfsNFSCopy.nodeSize = bytes_per_thread * uint64(concurrency)
	nfsNFSCopy.nodeOffset = uint64(nodeID) * nfsNFSCopy.nodeSize

	return nfsNFSCopy, nil
}

func (n *NFSInfo) SpreadCopy() (float64, []byte) {

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

	for i := 0; i < n.concurrency; i++ {

		// also we can't exceed the end of the file.
		max_bytes_to_read := n.sizeMB
		if max_bytes_to_read+offset > n.src_ff.size {
			max_bytes_to_read = n.src_ff.size - offset
		}

		var bar *mpb.Bar
		if n.progress {
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
		if n.copyv2 {
			go n.copyOneFileChunkv2(offset, max_bytes_to_read, i, bar)
		} else {
			go n.copyOneFileChunk(offset, max_bytes_to_read, i, bar)
		}

		offset += n.sizeMB
	}

	//Wait for all go routines to complete.
	n.wg.Wait()

	elapsed := time.Since(start)

	hasher := xxh3.New()
	hash_success := true
	for i := 0; i < len(n.hashes); i++ {
		if n.hashes[i] == nil {
			hash_success = false
			log.Warn("No read hash available ")
			break
		}
		log.Debugf("Thread %d hash: %x  offset: %d  bytes: %d",
			i+1, n.hashes[i], n.nodeOffset+uint64(i)*n.sizeMB, n.thread_bytes[i])
		hasher.Write(n.hashes[i])
	}
	var hashValue []byte
	if hash_success {
		hashValue = hasher.Sum([]byte{})
	} else {
		hashValue = nil
	}

	total_mb_bytes := atomic.LoadUint64(&n.atm_counter_bytes_written) / (1024 * 1024)

	fmt.Printf("Write Finished: Time: %f s , %d  MiB Transfered\n", elapsed.Seconds(), total_mb_bytes)

	return float64(total_mb_bytes) / (float64(elapsed.Seconds())), hashValue
}

func (n *NFSInfo) copyOneFileChunk(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

	defer n.wg.Done()

	//sleep time between threads
	//Avoids slamming server
	time.Sleep(time.Duration(threadID) * 2 * time.Millisecond)

	max_bytes_to_read := num_bytes

	var f_src ReadWriteSeekerCloser = nil
	var f_dst ReadWriteSeekerCloser = nil

	var err error = nil

	// Open the source file.
	f_src, err = n.src_ff.Open()
	if err != nil {
		log.Fatalf(" Thread %d Error opening source file: %s . \n Error: %s",
			threadID, n.src_ff.file_full_path, err)
		return
	}
	defer f_src.Close()

	// Open the Dest File
	f_dst, err = n.dst_ff.Open()
	if err != nil {
		log.Fatalf(" Thread %d Error opening destination file: %s . \n Error: %s",
			threadID, n.dst_ff.file_full_path, err)
		return
	}
	defer f_dst.Close()

	srcBuf := make([]byte, 1*1024*1024)

	hasher := xxh3.New()

	thread_bytes_written := uint64(0)
	thread_bytes_read := uint64(0)

	f_src.Seek(int64(offset), io.SeekStart)
	f_dst.Seek(int64(offset), io.SeekStart)

	for {
		if atomic.LoadInt32(&n.atm_finished) == 1 {
			break
		}

		remaining_bytes := max_bytes_to_read - thread_bytes_read
		bytes_to_read := uint64(len(srcBuf))

		if bytes_to_read > remaining_bytes {
			bytes_to_read = remaining_bytes
		}
		n_bytes, err := f_src.Read(srcBuf[0:bytes_to_read])
		thread_bytes_read += uint64(n_bytes)

		if err != nil {
			if !(err == io.EOF && n_bytes == int(bytes_to_read)) {
				fmt.Printf("Thread %d Error Read Error\n", threadID)
				fmt.Printf("%s\n", err)
				return
			}
		}
		if n.hash {
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
			if n.progress {
				bar.IncrBy(n_bytes_written)
			}

			if n_bytes_written_this_chunk == uint64(n_bytes) {
				break
			}
		}
		thread_bytes_written += n_bytes_written_this_chunk

		if thread_bytes_written == max_bytes_to_read {
			break
		}

		if thread_bytes_written > max_bytes_to_read {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	n.thread_bytes[threadID] = thread_bytes_written

	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
	atomic.AddUint64(&n.atm_counter_bytes_written, thread_bytes_written)
}

func (n *NFSInfo) copyOneFileChunkv2(offset uint64, num_bytes uint64, threadID int, bar *mpb.Bar) {

	defer n.wg.Done()

	//sleep time between threads
	//Avoids slamming server
	time.Sleep(time.Duration(threadID) * 2 * time.Millisecond)

	var f_src ReadWriteSeekerCloser = nil
	var f_dst ReadWriteSeekerCloserReaderFrom = nil

	var err error = nil

	// Open the source file.
	f_src, err = n.src_ff.Open()
	if err != nil {
		// Retry once,
		sleepfor := 100 + rand.Intn(50)
		log.Warnf("Thread %d will retry in %d miliseconds", threadID, sleepfor)
		time.Sleep(time.Duration(sleepfor) * time.Millisecond)
		f_src, err = n.src_ff.Open()
		if err != nil {
			log.Fatalf("Error opening source file: %s", n.src_ff.file_full_path)
		}
	}
	defer f_src.Close()

	// Open the Dest File
	f_dst, err = n.dst_ff.Open()

	if err != nil {
		// Retry once,
		sleepfor := 100 + rand.Intn(50)
		log.Warnf("Thread %d will retry in %d miliseconds", threadID, sleepfor)
		time.Sleep(time.Duration(sleepfor) * time.Millisecond)
		f_dst, err = n.dst_ff.Open()
		if err != nil {
			log.Fatalf("Error opening destination file: %s", n.dst_ff.file_full_path)
		}
	}
	defer f_dst.Close()

	f_src.Seek(int64(offset), io.SeekStart)
	f_dst.Seek(int64(offset), io.SeekStart)

	log.Debugf("Starting Thread %d using ReadFrom", threadID)

	// this may be faster on Linux, but on mac is 20% slower on large files
	//  because linux implements sendfile
	bytes_written, err := f_dst.ReadFrom(io.LimitReader(f_src, int64(num_bytes)))

	//testing vs copyv1, way more simple, but is it faster?
	//bytes_written, err := io.CopyN(f_dst, f_src, int64(num_bytes))
	if err != nil {
		log.Fatalf("Only Copied %d bytes in thread %d, at offset %d , Error: %s", bytes_written, threadID, offset, err)
	}
	log.Debugf(" Thread %d Wrote %d Bytes", threadID, bytes_written)

	atomic.AddUint64(&n.atm_counter_bytes_read, uint64(bytes_written))
	atomic.AddUint64(&n.atm_counter_bytes_written, uint64(bytes_written))
}
