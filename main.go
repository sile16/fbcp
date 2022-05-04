package main

import (
	//"bytes"
	"flag"
	"fmt"
	"os"
	"bytes"
	"runtime"

	//"github.com/vbauerster/mpb/v7"
	//"github.com/vbauerster/mpb/v7/decor"
)


func main() {

	nodesPtr := flag.Int("nodes",1, "Total number of nodes running to split workload across machines" )
	nodeIDPtr := flag.Int("node",0, "node ID number, 0 Indexed." )
	sizeMBPtr := flag.Int64("sizeMB", 128, "Number MB generated per thread during benchmark")
	threadsPtr := flag.Int("threads", 0, "Number of concurrent threads default is core count * 2")
	verifyPtr := flag.Bool("verify", false, "re-read data to calculate hashes to verify data trasnfer integrity.")
	//verbosePtr := flag.Bool("v", false, "Verbose output")
	benchmarkPtr := flag.Bool("benchmark", false, "Run a benchmark against a single file.")

	flag.Parse()

	nodes := *nodesPtr
	nodeID := *nodeIDPtr
	sizeMB := *sizeMBPtr
	threads := *threadsPtr
	benchmark := *benchmarkPtr
	verify := *verifyPtr

	pi, _ := os.Stdin.Stat() // get the FileInfo struct describing the standard input.
	po, _ := os.Stdout.Stat() // get the FileInfo struct describing the standard input.

	pipein, pipeout := false, false

	if (pi.Mode() & os.ModeNamedPipe) != 0 {
		//fmt.Println("data is from pipe")
		pipein = true
	} 
	if (po.Mode() & os.ModeNamedPipe) != 0 {
		//fmt.Println("data out is going to the pipe")
		pipeout = true
	}
	
	var src_ff *FlexFile
	var dst_ff *FlexFile
	var err error

	if benchmark{
		if flag.NArg() !=1 {
			flag.Usage()
			fmt.Print("Please provide a single test file.")
			os.Exit(0)
		}
		dst_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			fmt.Printf("Error opening destination file, %s", err)
			os.Exit(1)
		}

		coreCount := runtime.NumCPU()
		fmt.Printf("Found %d cores\n", coreCount)
		if threads == 0 {
			threads = coreCount * 2
		}

		if threads < coreCount * 2 {
			fmt.Printf("Recommend 2 threads / core, currently %d", threads )
		}

		nfs_bench, _ := NewNFSBench(dst_ff, threads, nodes, nodeID, uint64(sizeMB), verify)
		fmt.Println("Running NFS write test.")
		write_bytes_per_sec, hashValueWrite := nfs_bench.WriteTest()
		fmt.Printf("Write Throughput = %f MiB/s\n", write_bytes_per_sec)

		fmt.Println("Running NFS read test.")
		read_bytes_per_sec, hashValueRead := nfs_bench.ReadTest()
		fmt.Printf("Read Throughput = %f MiB/s\n", read_bytes_per_sec)

		if verify {
			fmt.Printf("Written Data Hash: %x\n", hashValueWrite )
			fmt.Printf("   Read Data Hash: %x\n", hashValueRead )

			if !bytes.Equal(hashValueRead, hashValueWrite) {
				fmt.Println("Error Error bad DATA !!!!!!!!!!!! ")
		}
		

		
	}

		os.Exit(0)
	}
	

	if pipein && !pipeout && flag.NArg() == 1 {
		src_ff, _ = NewFlexFilePipe(os.Stdin)
		dst_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			fmt.Printf("Error opening destination file, %s", err)
			os.Exit(1)
		}
	} else if !pipein && pipeout && flag.NArg() == 1 {
		src_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			fmt.Printf("Error opening source file, %s", err)
			os.Exit(1)
		}
		dst_ff, _ = NewFlexFilePipe(os.Stdout)
	} else if !pipein && !pipeout && flag.NArg() == 2 {
		src_ff, err = NewFlexFile(flag.Args()[0])
		if err != nil {
			fmt.Printf("Error opening source file, %s", err)
			os.Exit(1)
		}
		dst_ff, err = NewFlexFile(flag.Args()[1])
		if err != nil {
			fmt.Printf("Error opening destination file, %s", err)
			os.Exit(1)
		}
	} else if pipein && pipeout && flag.NArg() == 0 {
		src_ff, _ = NewFlexFilePipe(os.Stdin)
		dst_ff, _ = NewFlexFilePipe(os.Stdout)
	} else {
		flag.Usage()
		os.Exit(1)
	}

	if nodes > 1 && threads == 0{
		fmt.Printf("Missing threads param, required when using more than 1 node")
		return
	}	

	if threads == 0 {
		threads = 16
	}

	//var results []string
	//if pipein || pipeout {
	if true {
		nfs, err := NewNFSStream(src_ff, dst_ff, threads)
		if err != nil {
			fmt.Println(err)
			return
		}
		nfs.Stream()
		

	} else {
		nfs, err := NewNFSCopy(src_ff, dst_ff, threads, nodes, nodeID, verify)

		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("Running NFS MultiCopy.")
		copy_bytes_per_sec, hashValueWrite := nfs.SpreadCopy()
		fmt.Printf("Write Throughput = %f MiB/s\n", copy_bytes_per_sec)
		if verify {
			fmt.Printf("Written Data Hash: %x\n", hashValueWrite )
		}
		
		
	}



}
