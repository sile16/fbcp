package main

import (
	//"bytes"
	"flag"
	"fmt"
	"os"
	"bytes"
	"runtime"
	"runtime/pprof"
	log "github.com/sirupsen/logrus"
)


func main() {

	nodesPtr := flag.Int("nodes",1, "Total number of nodes running to split workload across machines" )
	nodeIDPtr := flag.Int("node",0, "node ID number, 0 Indexed." )
	sizeMBPtr := flag.Int64("sizeMB", 128, "Number MB generated per thread during benchmark")
	threadsPtr := flag.Int("threads", 0, "Number of concurrent threads default is core count * 2")
	verifyPtr := flag.Bool("verify", false, "re-read data to calculate hashes to verify data trasnfer integrity.")
	//verbosePtr := flag.Bool("v", false, "Verbose output")
	benchmarkPtr := flag.Bool("benchmark", false, "Run a benchmark against a single file.")
	zerosPtr := flag.Bool("zeros", false, "Benchmark Uses zeros instead of random data")
	readonlyPtr := flag.Bool("readonly", false, "Only read a file for the benchmark")
	verbosePtr := flag.Bool("verbose", false, "Turn on Verbose logging")
	forceInputStreamPtr := flag.Bool("force-input-stream", false, "Treat input file like a stream.")
	forceOutputStreamPtr := flag.Bool("force-output-stream", false, "Treat output file like a stream.")
	profile := flag.String("profile", "", "write cpu profile to specified file")
	copyv2 := flag.Bool("copyv2", false, "Use the io.copyN impklementiontation")
	stream := flag.Bool("stream", false, "Use the stream implementation")
	plaid := flag.Bool("plaid", false, "use plaid copy stream")

	flag.Parse()

	nodes := *nodesPtr
	nodeID := *nodeIDPtr
	sizeMB := *sizeMBPtr
	threads := *threadsPtr
	benchmark := *benchmarkPtr
	verify := *verifyPtr
	zeros := *zerosPtr
	readOnly := *readonlyPtr
	verbose := *verbosePtr
	forceInputStream := *forceInputStreamPtr
	forceOutputStream := *forceOutputStreamPtr

	if *profile != "" {
        f, err := os.Create(*profile)
        if err != nil {
            log.Fatal(err)
        }
		runtime.SetBlockProfileRate(1)
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }

	if verbose{ 
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

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
	
	// Flex file allows us to use a NFS path / local file or a Pipe transparentyly.
	var src_ff *FlexFile
	var dst_ff *FlexFile
	var err error

	if benchmark{
		//
		
		
		Benchmark, uses random data, or zeros to write to a file and read it back.
		if flag.NArg() !=1 {
			flag.Usage()
			log.Fatal("Please provide a single test file.")
		}
		dst_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening destination file, %s", err)
		}

		coreCount := runtime.NumCPU()
		fmt.Printf("Found %d cores\n", coreCount)
		if threads == 0 {
			threads = coreCount * 2
		}

		if threads < coreCount * 2 {
			log.Warningf("Recommend 2 threads / core, currently %d", threads )
		}

		nfs_bench, _ := NewNFSBench(dst_ff, threads, nodes, nodeID, uint64(sizeMB), verify, zeros)

		var write_bytes_per_sec float64
		var hashValueWrite []byte
		if !readOnly{
			log.Info("Running NFS write test.")
			write_bytes_per_sec, hashValueWrite = nfs_bench.WriteTest()
		}
		

		fmt.Println("Running NFS read test.")
		read_bytes_per_sec, hashValueRead := nfs_bench.ReadTest()
		if !readOnly{
			log.Infof("Write Throughput = %f MiB/s\n", write_bytes_per_sec)
		}
		
		log.Infof("Read Throughput = %f MiB/s\n", read_bytes_per_sec)

		if verify {
			log.Infof("   Read Data Hash: %x\n", hashValueRead )
			if !readOnly{
				log.Infof("Written Data Hash: %x\n", hashValueWrite )
				if !bytes.Equal(hashValueRead, hashValueWrite) {
					log.Error("Error Error bad DATA !!!!!!!!!!!! ")
				}
			}
		}

		os.Exit(0)
	}
	

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

	// this will use the code like it's reading from stdin
	if forceInputStream {
		src_ff.is_pipe = true
		pipein = true
	}

	// this will use the code like it's writing to stdout
	if forceOutputStream {
		dst_ff.is_pipe = true
		pipeout = true
	}

	if nodes > 1 && threads == 0{
		log.Fatal("Missing threads param, required when using more than 1 node")
		return
	}	

	if threads == 0 {
		threads = 16
	}

	if pipein || pipeout || *stream {
		log.Debugf("sourc: %t\n", src_ff.is_pipe)
		log.Debugf("dest:  %t\n", dst_ff.is_pipe)
		nfs, err := NewNFSStream(src_ff, dst_ff, threads, *plaid)
		if err != nil {
			fmt.Println(err)
			return
		}
		log.Info("Running a Stream Copy.")
		nfs.Stream()
	} else {
		nfs, err := NewNFSCopy(src_ff, dst_ff, threads, nodes, nodeID, verify, *copyv2)

		if err != nil {
			log.Fatal(err)
		}

		log.Info("Running NFS MultiCopy.")
		copy_bytes_per_sec, hashValueWrite := nfs.SpreadCopy()
		fmt.Printf("Write Throughput = %f MiB/s\n", copy_bytes_per_sec)
		if verify {
			fmt.Printf("Written Data Hash: %x\n", hashValueWrite )
		}
	}
}
