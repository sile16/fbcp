package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/sile16/go-nfs-client/nfs/metrics"
	log "github.com/sirupsen/logrus"
	xxh3 "github.com/zeebo/xxh3"
)

// Will return either zeros or random bytes.
type VirtualSourceReader struct {
	zeros bool
	size  int
	pos   int

	buf []byte
}

// create a new virtualsourcereader
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
		if single_read_size+vsr.pos > len(vsr.buf) {
			single_read_size = len(vsr.buf) - vsr.pos
		}

		//copy from buffer to p
		n := copy(p[bytes_read:bytes_read+single_read_size], vsr.buf[vsr.pos:vsr.pos+single_read_size])
		bytes_read += n
		bytes_left -= n
		vsr.pos += n
		if vsr.pos >= len(vsr.buf) {
			vsr.pos = 0
		}
	}
	return int(bytes_read), nil
}

// create virtualsource reader seek to offset
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

func benchmark(c Fbcp_config) {

	metrics.Metrics_init("fbcp_benchmark", 100)
	if c.csv {
		log.SetLevel(log.ErrorLevel)
	}
	//util.SetLevelDebug()

	if c.sizeMB == 0 {
		// default value for sizeMB
		c.sizeMB = 128
	}
	// Benchmark, uses random data, or zeros to write to a file and read it back.
	if flag.NArg() != 1 {
		flag.Usage()
		log.Fatal("Please provide a single test file.")
	}
	dst_ff, err := NewFlexFile(flag.Args()[0])
	if err != nil {
		log.Fatalf("Error opening destination file, %s", err)
	}

	log.Debug("Launching NFS Bench")
	nfs_bench, _ := NewNFSBench(dst_ff, &c)

	var write_bytes_per_sec float64
	var hashValueWrite []byte
	if !c.readOnly {
		log.Info("Running NFS write test.")
		write_bytes_per_sec, hashValueWrite = nfs_bench.WriteTest()
		log.Infof("Write Throughput = %f MiB/s\n", write_bytes_per_sec)
	}

	fmt.Println("Running NFS read test.")
	read_bytes_per_sec, hashValueRead := nfs_bench.ReadTest()
	log.Infof("Read Throughput = %f MiB/s\n", read_bytes_per_sec)

	if c.verify && hashValueRead != nil {
		log.Infof("   Read Data Hash: %x\n", hashValueRead)
		// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
		log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID+1, c.nodes)
		log.Infof("          Read Hash: %x\n", hashValueRead)
		if !c.readOnly {
			// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
			log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID+1, c.nodes)
			log.Infof("        Written Hash: %x\n", hashValueWrite)
			if !bytes.Equal(hashValueRead, hashValueWrite) {
				log.Error("Error Error bad DATA !!!!!!!!!!!! ")
			}
		}
	}

	if c.csv {
		if c.csv_header {
			fmt.Println("file,size,threads,node,nodes,io_depth,write_size,read_size,zeros,verify,read,write")
		}
		fmt.Printf("%s,%d,%d,%d,%d,%d,%d,%d,%t,%t,%f,%f",
			dst_ff.File_full_path, c.sizeMB, c.threads, c.nodeID, c.nodes,
			c.io_depth, c.max_write_size, c.max_read_size, c.zeros, c.verify,
			read_bytes_per_sec, write_bytes_per_sec)
	}

}

func NewNFSBench(dst_ff *FlexFile, c *Fbcp_config) (*NFSInfo, error) {
	// set to debug logging
	//util.DefaultLogger.SetDebug(true)

	nodeOffset := c.nodeID * c.threads * c.sizeMB * 1024 * 1024

	nfsBench := &NFSInfo{
		filesWritten: 0, dst_ff: dst_ff,
		hashes: make([][]byte, c.threads), nodeOffset: nodeOffset, c: c}

	total_file_size := c.nodes * c.threads * c.sizeMB * 1024 * 1024
	if !nfsBench.dst_ff.exists || nfsBench.dst_ff.Size != total_file_size {
		//truncate
		dst_ff.Truncate(int64(total_file_size))
	}

	return nfsBench, nil
}

func (n *NFSInfo) WriteTest() (float64, []byte) {
	atomic.StoreInt64(&n.atm_counter_bytes_written, 0)

	start := time.Now()
	offset := 0

	for i := 0; i < n.c.threads; i++ {
		n.wg.Add(1)
		go n.writeOneFileChunk(offset, i)
		offset += n.c.sizeMB * 1024 * 1024
	}
	n.wg.Wait()
	elapsed := time.Since(start)

	hasher := xxh3.New()
	for i := 0; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue := hasher.Sum([]byte{})

	total_bytes := atomic.LoadInt64(&n.atm_counter_bytes_written) / (1024 * 1024)

	log.Infof("Write Finished: Time: %f s , %d  MiB Transfered  %f MiB/s\n",
		elapsed.Seconds(),
		total_bytes,
		float64(total_bytes)/elapsed.Seconds())
	log.Infof("Written Data Hash: %x\n  ", hashValue)

	return float64(total_bytes) / (float64(elapsed.Seconds())), hashValue
}

func (n *NFSInfo) writeOneFileChunk(offset int, threadID int) {
	defer n.wg.Done()

	f, err := n.dst_ff.Open()
	if err != nil {
		log.Errorf("Error opening destination file. %s", err)
		return
	}
	defer f.Close()

	/*nfs_client, _ := nfs.NewNFSClient(n.dst_ff.Nfs_host, n.dst_ff.Export)

	f, err := nfs_client.Open(n.dst_ff.file_name)
	if err != nil {
		log.Errorf("Error opening destination file. %s", err)
		return
	}
	defer f.Close()*/

	f.SetMaxWriteSize((512) * 1024)
	f.SetIODepth(1)

	bytes_written := int64(0)

	vsr := NewVirtualSourceReader(int(8*1024*1024), n.c.zeros)
	vsr_limited := io.LimitReader(vsr, int64(n.c.sizeMB*1024*1024))

	_, err = f.Seek(int64(n.nodeOffset+offset), io.SeekStart)
	if err != nil {
		log.Errorf("Error seeking in destination file. %s", err)
		return
	}

	bytes_written, err = f.ReadFrom(vsr_limited)
	if err != nil {
		log.Errorf("Error writing to destination file. %s", err)
		return
	}

	if bytes_written != int64(n.c.sizeMB*1024*1024) {
		log.Errorf("Error writing to destination file. %d bytes written, expected %d bytes", bytes_written, n.c.sizeMB*1024*1024)
	}

	//todo; move this outside the timer
	hasher := xxh3.New()
	if n.c.hash {
		vsr.Seek(0, io.SeekStart)
		hash_vsr_limited := io.LimitReader(vsr, int64(n.c.sizeMB*1024*1024))
		io.CopyBuffer(hasher, hash_vsr_limited, nil)
	}

	n.hashes[threadID] = hasher.Sum([]byte{})
	atomic.AddInt64(&n.atm_counter_bytes_written, bytes_written)
}

func (n *NFSInfo) ReadTest() (float64, []byte) {
	log.Infof("Starting Read Test")

	atomic.StoreInt64(&n.atm_counter_bytes_read, 0)

	start := time.Now()
	offset := 0

	for i := 0; i < n.c.threads; i++ {
		log.Infof("Starting thread %d\n", i)
		n.wg.Add(1)
		go n.readOneFileChunk(offset, i)
		n.offset += n.c.sizeMB * 1024 * 1024
	}

	n.wg.Wait()

	hasher := xxh3.New()

	for i := 0; i < len(n.hashes); i++ {
		hasher.Write(n.hashes[i])
	}
	hashValue := hasher.Sum([]byte{})

	elapsed := time.Since(start)
	total_bytes := atomic.LoadInt64(&n.atm_counter_bytes_read) / (1024 * 1024)

	return float64(total_bytes) / float64(elapsed.Seconds()), hashValue
}

func (n *NFSInfo) readOneFileChunk(offset int, threadID int) {
	defer n.wg.Done()
	log.Infof("starting readOneFileChunk")

	//p := make([]byte, 512*1024)
	//byte_counter := uint64(0)

	f, err := n.dst_ff.Open()
	if err != nil {
		fmt.Print("Error opening destination file.")
		return
	}
	defer f.Close()

	/*
		nfs_client, _ := nfs.NewNFSClient(n.dst_ff.Nfs_host, n.dst_ff.Export)

		f, err := nfs_client.Open(n.dst_ff.file_name)

		if err != nil {
			log.Error("Error opening destination file.")
			return
		} */

	f.Seek(int64(n.nodeOffset+offset), io.SeekStart)

	f.SetIODepth(3)
	f.SetMaxReadSize(512 * 1024)

	log.Infof("Thread: %d Reading from offset %d", threadID, n.nodeOffset+offset)

	var n_bytes int64
	var err2 error

	if n.c.hash {
		hasher := xxh3.New()
		n_bytes, err2 = f.WriteTo(hasher)
		n.hashes[threadID] = hasher.Sum([]byte{})
	} else {
		n_bytes, err2 = f.WriteTo(io.Discard)
	}

	if err2 != nil {
		log.Errorf("Thread %d Error: %s \n", threadID, err2)
	}

	if int(n_bytes) == n.c.sizeMB*1024*1024 {
		log.Infof("ThreadID %d All bytes read \n", threadID)
	}

	/*
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
		}*/

	log.Debugf("Thread Read %d - Done!!!!! \n", threadID)
	atomic.AddInt64(&n.atm_counter_bytes_read, n_bytes)
}
