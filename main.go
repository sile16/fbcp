package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"github.com/shirou/gopsutil/v3/mem"

	log "github.com/sirupsen/logrus"
)

const min_thread_size uint64 = uint64(16) * 1024 * 1024
const default_stream_sizeMB int64 = 8
const min_stream_sizeMB int64 = 4
var max_stream_memG int64 = 8


func getFreeMemoryGB() (uint64, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return v.Available / (1024 * 1024 * 1024), nil
}

type fbcp_config struct { 
	nodes int
	nodeID int
	sizeMB int64
	threads int
	benchmark bool
	verify bool
	zeros bool
	readOnly bool
	verbose bool
	forceInputStream bool
	forceOutputStream bool
	hash bool
	showmounts bool
	profile string
	sendfile bool
	no_src_direct_nfs bool
	no_dst_direct_nfs bool
	plaid bool
	progress bool
}


func hash(c fbcp_config) {
	//Same checks as benchamrk , single file passed, so we 
	// Note that we can't do a streaming hash, as that is pointless just use xxhsum
	if c.threads == 0 {
		log.Fatalf("Threads must be specified for a correct hash.")
	}

	src_ff, err := NewFlexFile(flag.Args()[0])
	if err != nil {
		log.Fatalf("Error opening destination file, %s", err)
	}

	nfs, err := NewSpreadHash(src_ff, c.threads, c.nodes, c.nodeID, uint64(c.sizeMB) * 1024 *1024, c.progress)
	if err != nil {
		log.Fatal(err)
	}

	copy_bytes_per_sec, hashValueWrite := nfs.SpreadHash()
	fmt.Printf("Read Throughput = %f MiB/s\n", copy_bytes_per_sec)
	// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
	fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID + 1, c.nodes)
	fmt.Printf("       Hash: %x\n", hashValueWrite )

}

func benchamrk(c fbcp_config ){

	if c.sizeMB == 0 {
		// default value for sizeMB
		c.sizeMB = 128
	}
	// Benchmark, uses random data, or zeros to write to a file and read it back.
	if flag.NArg() !=1 {
		flag.Usage()
		log.Fatal("Please provide a single test file.")
	}
	dst_ff, err := NewFlexFile(flag.Args()[0])
	if err != nil {
		log.Fatalf("Error opening destination file, %s", err)
	}

	//if c.threads < coreCount * 2 {
	//	log.Warningf("Recommend 2 threads / core, currently %d for %d cores", c.threads, coreCount )
	//}

	log.Debug("Launching NFS Bench")
	nfs_bench, _ := NewNFSBench(dst_ff, c.threads, c.nodes, c.nodeID, uint64(c.sizeMB), c.verify, c.zeros)

	var write_bytes_per_sec float64
	var hashValueWrite []byte
	if !c.readOnly{
		log.Debug("Running NFS write test.")
		write_bytes_per_sec, hashValueWrite = nfs_bench.WriteTest()
	}

	fmt.Println("Running NFS read test.")
	read_bytes_per_sec, hashValueRead := nfs_bench.ReadTest()
	if !c.readOnly{
		log.Infof("Write Throughput = %f MiB/s\n", write_bytes_per_sec)
	}
	
	log.Infof("Read Throughput = %f MiB/s\n", read_bytes_per_sec)

	if c.verify && hashValueRead != nil {
		log.Infof("   Read Data Hash: %x\n", hashValueRead )
		// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
		fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID +1, c.nodes)
		fmt.Printf("          Read Hash: %x\n", hashValueRead )
		if !c.readOnly{
			// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
			fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID + 1, c.nodes)
			log.Infof("        Written Hash: %x\n", hashValueWrite )
			if !bytes.Equal(hashValueRead, hashValueWrite) {
				log.Error("Error Error bad DATA !!!!!!!!!!!! ")
			}
		}
	}

}

func showmounts() {

	mounts, _ := getMounts()
	for _, mount_entry := range mounts{
		fmt.Printf("Device: '%s'\n", mount_entry.device )
		fmt.Printf("Mount Point: '%s'\n", mount_entry.mount_point)
		fmt.Printf("Proto: %s, nfs %t , Nconnect: %t, nconnect_count: %d\n\n",
			mount_entry.protocol, mount_entry.nfs,
			mount_entry.nconnect, mount_entry.nconnect_value)

	}
}

func fbcp_stream_copy(c fbcp_config, src_ff *FlexFile, dst_ff *FlexFile) {
	log.Debugf("sourc: %t\n", src_ff.is_pipe)
	log.Debugf("dest:  %t\n", dst_ff.is_pipe)

	// do an auto sizing of sizeMB
	if c.sizeMB == 0 {
		c.sizeMB = int64(default_stream_sizeMB)

		// if we are streaming to a file, we need to make sure we have memory
		free_mem, _ := getFreeMemoryGB()

		if  int64(free_mem)  <  max_stream_memG + 1 {
			max_stream_memG = int64(free_mem)- 1
			log.Debugf("Free Memory of %d G, setting max memory to %d", free_mem, max_stream_memG)
		}

		if c.sizeMB * int64(c.threads) > max_stream_memG * 1024 {
			c.sizeMB = max_stream_memG * 1024 / int64(c.threads)
			log.Debug("Limiting sizeMB to be below max memory of % G", max_stream_memG)
			// Round DOWN to nearest 16MB
			c.sizeMB = ( c.sizeMB / min_stream_sizeMB ) * min_stream_sizeMB
			if c.sizeMB <= 0 {
				c.sizeMB = min_stream_sizeMB
			}
		}
		log.Debug(" sizeMB was not specificed setting to: ", c.sizeMB)
	}

	if c.threads == 0 {
		c.threads = 16
	}

	nfs, err := NewNFSStream(src_ff, dst_ff, c.threads, c.plaid, uint64(c.sizeMB))
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Debug("Running a Stream Copy.")
	nfs.Stream()
}

func fbcp_copy(c fbcp_config, src_ff *FlexFile, dst_ff *FlexFile) {

	var hashValueRead []byte
	var hashValueWrite []byte
	var copy_bytes_per_sec float64

	if c.verify && !c.hash {
		//have to hash the source to verify the destination file.
		c.hash = true
	}

	nfs, err := NewNFSCopy(src_ff, dst_ff, c.threads, c.nodes, c.nodeID, uint64(c.sizeMB) * 1024 * 1024,
	 c.hash, c.sendfile, c.progress)
	if err != nil {
		log.Fatal(err)
	}

	log.Info("Running NFS MultiCopy.")
	copy_bytes_per_sec, hashValueWrite = nfs.SpreadCopy()
	log.Infof("Write Throughput = %f MiB/s\n", copy_bytes_per_sec)
	if c.hash && hashValueWrite != nil{
		// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
		log.Infof("Source File Hash: %s", src_ff.file_full_path)
		log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ",c. threads, c.nodeID + 1, c.nodes)
		log.Infof("       Hash: %x\n", hashValueWrite )
	}

	if c.verify {
		dst_ff_verify, err := NewFlexFile(flag.Args()[1])
		if err != nil {
			log.Fatalf("Error opening destination file for verfication!, %s", err)
		}

		nfs, err := NewSpreadHash(dst_ff_verify, c.threads, c.nodes, c.nodeID, uint64(c.sizeMB) * 1024 *1024, c.progress)
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("Reading back the destination file to verify hash.")
		copy_bytes_per_sec, hashValueRead = nfs.SpreadHash()
		log.Infof("Read Throughput = %f MiB/s\n", copy_bytes_per_sec)
		log.Infof("Destination File Hash: %s", dst_ff_verify.file_full_path)
		
		// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
		log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ", c.threads, c.nodeID + 1, c.nodes)
		log.Infof("       Hash: %x\n", hashValueWrite )

		if !bytes.Equal(hashValueRead, hashValueWrite) {
			log.Fatal("Error bad DATA !!!!!!!!!!!! Hash mistmatch ")
		} else {
			log.Info("!! Data Validated !!  Hashes are correct.")
		}
	} else if c.hash {
		log.Info("Only source file hashed, destination file NOT checked, use -verify to automatically check destination file.")
	}
}

func fbcp(c fbcp_config) {
	// Copy file to file,
	pi, _ := os.Stdin.Stat() // get the FileInfo struct describing the standard input.
	po, _ := os.Stdout.Stat() // get the FileInfo struct describing the standard input.

	pipein, pipeout := false, false

	if (pi.Mode() & os.ModeNamedPipe) != 0 {
		log.Debug("Data in is from pipe")
		pipein = true
	} 
	if (po.Mode() & os.ModeNamedPipe) != 0 {
		log.Debug("Data out is to a pipe")
		pipeout = true
	}
	
	// Flex file allows us to use a NFS path / local file or a Pipe transparently.
	var src_ff *FlexFile
	var dst_ff *FlexFile
	var err error

	//default_thread_used := false
	coreCount := runtime.NumCPU()
	if c.threads == 0 {
		log.Infof("Found %d cores\n", coreCount)
		c.threads = coreCount * 2
		if c.threads > 48 {
			c.threads = 48
		}
	}

	//create flex file for src and dst
	if pipein && !pipeout && flag.NArg() == 1 {
		src_ff, _ = NewFlexFilePipe(os.Stdin)
		dst_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening destination file, %s", err)
		}
	} else if !pipein && pipeout && flag.NArg() == 1 {
		src_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening source file, %s", err)
		}
		dst_ff, _ = NewFlexFilePipe(os.Stdout)
	} else if !pipein && !pipeout && flag.NArg() == 2 {
		src_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening source file, %s", err)
		}
		dst_ff, err = NewFlexFile(flag.Args()[1])
		if err != nil {
			log.Fatalf("Error opening destination file, %s", err)
		}
	} else if pipein && pipeout && flag.NArg() == 0 {
		src_ff, _ = NewFlexFilePipe(os.Stdin)
		dst_ff, _ = NewFlexFilePipe(os.Stdout)
	} else {
		flag.Usage()
		os.Exit(1)
	}

	// See if we can find the direct NFS mount for the source 
	if !src_ff.is_nfs && !c.no_src_direct_nfs{
		log.Debug("Trying to find direct NFS mount for source")
		src_ff = NewFlexFileDirectNFS(src_ff)
	}

	// See if we can find the direct NFS mount for the destination 
	if !dst_ff.is_nfs && !c.no_dst_direct_nfs{
		log.Debug("Trying to find direct NFS mount for destination")
		dst_ff = NewFlexFileDirectNFS(dst_ff)
	}

	// this will use the code like it's reading from stdin
	// use the streaming code even though its a file.
	if c.forceInputStream {
		log.Debug("Forcing input to streaming")
		src_ff.is_pipe = true
		pipein = true
	}

	// this will use the code like it's writing to stdout
	if c.forceOutputStream {
		log.Debug("Forcing output to streaming")
		dst_ff.is_pipe = true
		pipeout = true
	}

	if c.threads == 0 {
		c.threads = 16
	}

	if pipein || pipeout {
		fbcp_stream_copy(c, src_ff, dst_ff)

	} else {
		fbcp_copy(c, src_ff, dst_ff)
	}

}
 

func main() {
	var c fbcp_config

	flag.BoolVar(&c.benchmark, "benchmark", false, "Run a benchmark against a single file.")
	flag.BoolVar(&c.hash, "hash", false, "This will only hash a file, sizeMB & threads are required.")

	flag.IntVar(&c.nodes, "nodes",1, "Total number of nodes running to split workload across machines" )
	flag.IntVar(&c.nodeID, "node",1, "node ID number, first node starts at 1." )
	flag.Int64Var(&c.sizeMB, "sizeMB", 0, "Number MB generated per thread during benchmark")
	flag.IntVar(&c.threads, "threads", 0, "Number of concurrent threads default is core count * 2")
	flag.BoolVar(&c.verify, "verify", false, "re-read data to calculate hashes to verify data trasnfer integrity.")
	flag.BoolVar(&c.zeros, "zeros", false, "Benchmark Uses zeros instead of random data")
	flag.BoolVar(&c.readOnly, "readonly", false, "Only read a file for the benchmark")
	flag.BoolVar(&c.verbose, "verbose", false, "Turn on Verbose logging")
	flag.BoolVar(&c.forceInputStream, "force-input-stream", false, "Treat input file like a stream.")
	flag.BoolVar(&c.forceOutputStream, "force-output-stream", false, "Treat output file like a stream.")
	flag.StringVar(&c.profile, "profile", "", "write cpu profile to specified file")
	flag.BoolVar(&c.sendfile, "sendfile", false, "Use the io.copyN impklementiontation")
	flag.BoolVar(&c.no_src_direct_nfs, "no-src-direct-nfs", false, "Do not try direct nfs to source")
	flag.BoolVar(&c.no_dst_direct_nfs, "no-dst-direct-nfs", false, "Do not try direct nfs to destination")
	flag.BoolVar(&c.plaid, "plaid", false, "use plaid copy stream")
	flag.BoolVar(&c.progress, "progress", false, "Show progress bars")
	flag.BoolVar(&c.showmounts, "showmounts", false, "Only Display Mounts")

	//internally nodeID is 0 indexed, but for command line it's 1 indexed
	c.nodeID--

	flag.Parse()

	if c.verbose{ 
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if c.profile != "" {
        f, err := os.Create(c.profile)
        if err != nil {
            log.Fatal(err)
        }
		runtime.SetBlockProfileRate(1)
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

	if c.nodes > 1 && c.threads == 0{
		log.Fatal("Missing threads param, required when using more than 1 node")
		return
	}

	if c.showmounts {
		showmounts()
		os.Exit(0)
	}
	
	//hash 
	if c.hash && flag.NArg() == 1 {
		hash(c)	
		os.Exit(0)
	}

	//benchmark
	if c.benchmark{
		benchamrk(c)
		os.Exit(0)
	}

	//else fbcp
	fbcp(c)
}
