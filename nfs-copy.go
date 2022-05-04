package main

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/joshuarobinson/go-nfs-client/nfs"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

func NewNFSCopy(src_ff *FlexFile, dst_ff *FlexFile, concurrency int, nodes int, nodeID int ) (*NFSInfo, error) {

	nfsNFSCopy := &NFSInfo{
		src_ff: src_ff, dst_ff: dst_ff, 
		concurrency: concurrency, filesWritten: 0,
	    hashes: make([][]byte, concurrency)}
	
	if !nfsNFSCopy.src_ff.exists {
		fmt.Printf("Error: source fle %s doesn't exist", nfsNFSCopy.src_ff.file_name)
		os.Exit(1)
	}

	if nfsNFSCopy.dst_ff.is_directory {
		fmt.Println("Destination file is a directory.")
		os.Exit(1)
	}

	/*
	if nfsNFSCopy.nodeOffset == 0 && nfsNFSCopy.dst_ff.exists {
		fmt.Println("Destination file already exists.")
		os.Exit(1)
	}*/

	//todo; make it work on directories, right now we will be explicit
	/*if nfsNFSCopy.dst_ff.is_directory{

		nfsNFSCopy.dst_ff.file_path = path.Join(nfsNFSCopy.dst_ff.file_path,
									
			                                    nfsNFSCopy.dst_ff.file_name)
												
		nfsNFSCopy.dst_ff.file_name = nfsNFSCopy.src_ff.file_path										
	}*/

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
		//fname := generateTestFilename(n.uniqueId, i)
		n.wg.Add(1)
		
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
		go n.copyOneFileChunk(offset, max_bytes_to_read, i, bar)
		offset += n.sizeMB

	}

	// This timer is more like a time out value now,
	//time.Sleep(time.Duration(n.durationSeconds) * time.Second)
	//atomic.StoreInt32(&n.atm_finished, 1)
	n.wg.Wait()
	n.filesWritten += n.concurrency



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

	if n.src_ff.is_nfs {
		mount_src, err := nfs.DialMount(n.src_ff.nfs_host, true)
		if err != nil {
			fmt.Println("Portmapper failed.")
			fmt.Println(err)
			return
		}
		defer mount_src.Close()
		
		target_src, err := mount_src.Mount(n.src_ff.export, n.authUnix.Auth(), true)
		if err != nil {
			fmt.Println("Unable to mount.")
			fmt.Println(err)
			return
		}
		defer target_src.Close()

		f_src, err = target_src.OpenFile(n.src_ff.file_name, os.FileMode(int(0644)))
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", n.src_ff.file_name)
			fmt.Println(err)
			return
		}
		defer f_src.Close()

	} else {
		f_src, err = os.Open(n.src_ff.file_full_path)
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", n.src_ff.file_full_path)
			fmt.Println(err)
			return
		}
		defer f_src.Close()
	}

	// Open the Dest File
	if n.dst_ff.is_nfs {
		mount_dst, err := nfs.DialMount(n.dst_ff.nfs_host, true)
		if err != nil {
			fmt.Println("Portmapper failed.")
			fmt.Println(err)
			return
		}
		defer mount_dst.Close()
		
		target_dst, err := mount_dst.Mount(n.dst_ff.export, n.authUnix.Auth(), true)
		if err != nil {
			fmt.Println("Unable to mount.")
			fmt.Println(err)
			return
		}
		defer target_dst.Close()

		f_dst, err = target_dst.OpenFile(n.dst_ff.file_name, os.FileMode(int(0777)))
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", n.dst_ff.file_name)
			fmt.Println(err)
			return
		}
		

	} else {
		f_dst, err = os.Create(n.dst_ff.file_full_path)
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", n.dst_ff.file_full_path)
			fmt.Println(err)
			return
		}

	}
	

	srcBuf := make([]byte,32*1024*1024)

	hasher := md5.New()
	
	thread_bytes_written := uint64(0)
	thread_bytes_read := uint64(0)

	f_src.Seek(int64(offset), io.SeekStart)
	f_dst.Seek(int64(offset), io.SeekStart)
	


	//proxyReader := bar.ProxyReader(f_dst)
	//defer proxyReader.Close()

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
				f_dst.Close()
				return
			}
		}
		
		hasher.Write(srcBuf[0:n_bytes])
		n_bytes_written, err := f_dst.Write(srcBuf[0:n_bytes])
		thread_bytes_written += uint64(n_bytes_written)
		bar.IncrBy(n_bytes_written)

		

		if err != nil {
			fmt.Printf("Thread %d Error: write error!\n", threadID)
			break
		}

		if n_bytes_written != n_bytes {
			fmt.Printf("Thread %d Warning: Not all bytes written!\n", threadID)
			break
		}

		if thread_bytes_written == max_bytes_to_read {
			break
		}
		
		if  thread_bytes_written > max_bytes_to_read {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}

		if err == io.EOF {
			fmt.Printf("Thread %d Warning: Unexpected End of File! \n", threadID)
			break
		}

	}
	f_dst.Close()
	

	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_read, thread_bytes_read)
	
	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddUint64(&n.atm_counter_bytes_written, thread_bytes_written)
}


/*
func (n *NFSCopy) ReadTest() ( float64, []byte ) {

	if n.filesWritten == 0 {
		fmt.Println("[error] Unable to perform ReadTest, no files written.")
		return float64(0), nil
	}
	atomic.StoreInt32(&n.atm_finished, 0)
	atomic.StoreUint64(&n.atm_counter_bytes_read, 0)

	start := time.Now()

	offset := int64(0)
	for i := 0; i < n.filesWritten; i++ {
		n.wg.Add(1)
		go n.readOneFileChunk(n.dstFile, offset, n.sizeMB, i)
		offset += n.sizeMB
	}

	//time.Sleep(time.Duration(n.durationSeconds) * time.Second)
	//atomic.StoreInt32(&n.atm_finished, 1)
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


func (n *NFSCopy) readOneFileChunk(fname string, offset int64, sizeMB int64, threadID int) {

	defer n.wg.Done()

	mount, err := nfs.DialMount(n.nfshost, true)
	if err != nil {
		fmt.Println(err)
		return
	}
	
	target, err := mount.Mount(n.export, n.authUnix.Auth(), true)
	if err != nil {
		fmt.Println("err")
		return
	}
	defer target.Close()

	hasher := md5.New()
	p := make([]byte, 512*1024)
	byte_counter := uint64(0)

	f, err := target.Open(fname)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	f.Seek((n.nodeOffset + offset) * 1024 * 1024, io.SeekStart)
	half := false

	for {
		if atomic.LoadInt32(&n.atm_finished) == 1 {
			break
		}

		n_bytes, err := f.Read(p)
		hasher.Write(p)
		byte_counter += uint64(n_bytes)
		

		if byte_counter == uint64(sizeMB)*1024*1024 {
			fmt.Printf("Thread Read %d - Done!!!!!\n", threadID)
			break
		}
		
		if  byte_counter > uint64(sizeMB)*1024*1024 {
			fmt.Printf("Thread %d Warning: Read more bytes than expected \n", threadID)
			break
		}

		if err == io.EOF {
			fmt.Printf("Thread %d Warning: Unexpected End of File! \n", threadID)
			break
		}

		if !half && byte_counter >= uint64(sizeMB)*1024*1024/2 {
			fmt.Printf("Thread Read %d - 50%%\n", threadID)
			half = true
		}
		
		
	}
	n.hashes[threadID] = hasher.Sum([]byte{})
 	fmt.Printf("Thread Read %d - Done!!!!! \n", threadID)
	atomic.AddUint64(&n.atm_counter_bytes_read, byte_counter)
}

*/