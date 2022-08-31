package main

import (
	//"bytes"
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"
	//"runtime/debug"
	"runtime/pprof"

	log "github.com/sirupsen/logrus"
)

const min_thread_size uint64 = uint64(16) * 1024 * 1024


func main() {

	benchmarkPtr := flag.Bool("benchmark", false, "Run a benchmark against a single file.")
	hashPtr := flag.Bool("hash", false, "This will only hash a file, sizeMB & threads are required.")

	nodesPtr := flag.Int("nodes",1, "Total number of nodes running to split workload across machines" )
	nodeIDPtr := flag.Int("node",1, "node ID number, first node starts at 1." )
	sizeMBPtr := flag.Int64("sizeMB", 0, "Number MB generated per thread during benchmark")
	threadsPtr := flag.Int("threads", 0, "Number of concurrent threads default is core count * 2")
	verifyPtr := flag.Bool("verify", false, "re-read data to calculate hashes to verify data trasnfer integrity.")
	zerosPtr := flag.Bool("zeros", false, "Benchmark Uses zeros instead of random data")
	readonlyPtr := flag.Bool("readonly", false, "Only read a file for the benchmark")
	verbosePtr := flag.Bool("verbose", false, "Turn on Verbose logging")
	forceInputStreamPtr := flag.Bool("force-input-stream", false, "Treat input file like a stream.")
	forceOutputStreamPtr := flag.Bool("force-output-stream", false, "Treat output file like a stream.")
	profile := flag.String("profile", "", "write cpu profile to specified file")
	sendfile := flag.Bool("sendfile", false, "Use the io.copyN impklementiontation")
	stream := flag.Bool("stream", false, "Use the stream implementation")
	plaid := flag.Bool("plaid", false, "use plaid copy stream")
	progressPtr := flag.Bool("progress", false, "Show progress bars")
	showmountsPtr := flag.Bool("showmounts", false, "Only Display Mounts")

	flag.Parse()

	nodes := *nodesPtr
	nodeID := *nodeIDPtr - 1
	sizeMB := *sizeMBPtr
	threads := *threadsPtr
	benchmark := *benchmarkPtr
	verify := *verifyPtr
	zeros := *zerosPtr
	readOnly := *readonlyPtr
	verbose := *verbosePtr
	forceInputStream := *forceInputStreamPtr
	forceOutputStream := *forceOutputStreamPtr
	hash := *hashPtr
	
	if *showmountsPtr{

		mounts, _ := getMounts()
		for _, mount_entry := range mounts{
			fmt.Printf("Device: '%s'\n", mount_entry.device )
			fmt.Printf("Mount Point: '%s'\n", mount_entry.mount_point)
			fmt.Printf("Proto: %s, nfs %t , Nconnect: %t, nconnect_count: %d\n\n",
				mount_entry.protocol, mount_entry.nfs,
				mount_entry.nconnect, mount_entry.nconnect_value)

		}
		return
	}

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
	var hashValueRead []byte
	var hashValueWrite []byte
	var copy_bytes_per_sec float64

	//default_thread_used := false
	coreCount := runtime.NumCPU()
	if threads == 0 {
		log.Infof("Found %d cores\n", coreCount)
		threads = coreCount * 2
		//default_thread_used = true
	}

	if hash && flag.NArg() == 1 {
		//Same checks as benchamrk , single file passed, so we 
		// Note that we can't do a streaming hash, as that is pointless just use xxhsum
		if threads == 0 {
			log.Fatalf("Threads must be specified for a correct hash.")
		}

		src_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening destination file, %s", err)
		}

		nfs, err := NewSpreadHash(src_ff, threads, nodes, nodeID, uint64(sizeMB) * 1024 *1024, *progressPtr)
		if err != nil {
			log.Fatal(err)
		}

		copy_bytes_per_sec, hashValueWrite := nfs.SpreadHash()
		fmt.Printf("Read Throughput = %f MiB/s\n", copy_bytes_per_sec)
		// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
		fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", threads, nodeID + 1, nodes)
		fmt.Printf("       Hash: %x\n", hashValueWrite )
			
		os.Exit(0)
	}


	if benchmark{
		if sizeMB == 0 {
			// default value for sizeMB
			sizeMB = 128
		}
		// Benchmark, uses random data, or zeros to write to a file and read it back.
		if flag.NArg() !=1 {
			flag.Usage()
			log.Fatal("Please provide a single test file.")
		}
		dst_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			log.Fatalf("Error opening destination file, %s", err)
		}

		if threads < coreCount * 2 {
			log.Warningf("Recommend 2 threads / core, currently %d for %d cores", threads, coreCount )
		}

		log.Debug("Launching NFS Bench")
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

		if verify && hashValueRead != nil {
			log.Infof("   Read Data Hash: %x\n", hashValueRead )
			// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
			fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", threads, nodeID + 1, nodes)
			fmt.Printf("          Read Hash: %x\n", hashValueRead )
			if !readOnly{
				// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
				fmt.Printf("Spread Hash Threads: %d, Hash Node %d of %d ", threads, nodeID + 1, nodes)
				log.Infof("        Written Hash: %x\n", hashValueWrite )
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
		log.Debug("Forcing input to streaming")
		src_ff.is_pipe = true
		pipein = true
	}

	// this will use the code like it's writing to stdout
	if forceOutputStream {
		log.Debug("Forcing output to streaming")
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
		
		if !src_ff.is_nfs {
			nfs_path := getNFSPathFromLocal(src_ff.file_full_path)
			if nfs_path != "" {
				log.Debugf("found local_path: %s, \n      at nfs_path: %s",
			             src_ff.file_full_path, nfs_path)
				new_nfs_ff, err := NewFlexFile(nfs_path)
				if err != nil {
					log.Debugf("Error creating flex file for %s", nfs_path)
					log.Debug("Falling back to local path")
				} else {
					log.Debug("Changing src to direct NFS")
					src_ff = new_nfs_ff
				}
			}
		}

		if !dst_ff.is_nfs {
			nfs_path := getNFSPathFromLocal(dst_ff.file_full_path)
			if nfs_path != "" {
				log.Debugf("found local_path: %s, \n      at nfs_path: %s",
			             dst_ff.file_full_path, nfs_path)
				new_nfs_ff, err := NewFlexFile(nfs_path)
				if err != nil {
					log.Debugf("Error creating flex file for %s", nfs_path)
					log.Debug("Falling back to local path")
				} else {
					log.Debug("Changing dst to direct NFS")
					dst_ff = new_nfs_ff
				}
			}
		}

		if verify && !hash {
			//have to hash the source to verify the destination file.
			hash = true
		}

		nfs, err := NewNFSCopy(src_ff, dst_ff, threads, nodes, nodeID, uint64(sizeMB) * 1024 * 1024,
			hash, *sendfile, *progressPtr)
		if err != nil {
			log.Fatal(err)
		}

		log.Info("Running NFS MultiCopy.")
		copy_bytes_per_sec, hashValueWrite = nfs.SpreadCopy()
		log.Infof("Write Throughput = %f MiB/s\n", copy_bytes_per_sec)
		if hash && hashValueWrite != nil{
			// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
			log.Infof("Source File Hash: %s", src_ff.file_full_path)
			log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ", threads, nodeID + 1, nodes)
			log.Infof("       Hash: %x\n", hashValueWrite )
		}

		if verify {
			dst_ff_verify, err := NewFlexFile(flag.Args()[1])
			if err != nil {
				log.Fatalf("Error opening destination file for verfication!, %s", err)
			}

			nfs, err := NewSpreadHash(dst_ff_verify, threads, nodes, nodeID, uint64(sizeMB) * 1024 *1024, *progressPtr)
			if err != nil {
				log.Fatal(err)
			}

			log.Infof("Reading back the destination file to verify hash.")
			copy_bytes_per_sec, hashValueRead = nfs.SpreadHash()
			log.Infof("Read Throughput = %f MiB/s\n", copy_bytes_per_sec)
			log.Infof("Destination File Hash: %s", dst_ff_verify.file_full_path)
			
			// add 1 to nodeID, to change from 0 Indexed to 1 indexed.
			log.Infof("Spread Hash Threads: %d, Hash Node %d of %d ", threads, nodeID + 1, nodes)
			log.Infof("       Hash: %x\n", hashValueWrite )

			if !bytes.Equal(hashValueRead, hashValueWrite) {
				log.Fatal("Error bad DATA !!!!!!!!!!!! Hash mistmatch ")
			} else {
				log.Info("!! Data Validated !!  Hashes are correct.")
			}
		} else if hash {
			log.Info("Only source file hashed, destination file NOT checked, use -verify to automatically check destination file.")
		}
	}
}
