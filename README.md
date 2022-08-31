# fbcp
FlashBlade Fast File transfer tool (benchmark tool, hashing tool, pipe streaming tool)

Created to help speed up large file transfers.

This tool currently only works with single files or pipes for source and destination.  If the source or destination file is on an NFS mount.  fbcp will automatically try to connect directly to the NFS server using a built in user space NFS client.  This bypasses the system linux kernel NFS client implementation.  If the connection fails it will fallback to a regular file system open command.

Two different copy engines are implemented, with other options in each one, i.e. read chunk sizes 
linux sendfile usage, local FS vs direct NFS, thread count etc.   Currently the tool can deliver up 
to 5 GiB/s of transfer speeds for a single file on an NFS server.

This is a beta tool and commands and params will change in the future, specifically benchmark and hash and possibly streaming to be put into seperate binaries in the future.

Please hash your source and destination files to ensure file integrity.  While this tool has been tested with 100's of automated tests ("go test"), file integrity is not guaranteed by this tool or author.    

## Usage

4 main modes of operation that are completley seperate, file copy mode, streaming mode, hash and benchmark.

```
Usage of ./fbcp/fbcp_mac:
  -benchmark
    	Run a benchmark against a single file.
  -force-input-stream
    	Treat input file like a stream.
  -force-output-stream
    	Treat output file like a stream.
  -hash
    	This will only hash a file, sizeMB & threads are required.
  -node int
    	node ID number, first node starts at 1. (default 1)
  -nodes int
    	Total number of nodes running to split workload across machines (default 1)
  -plaid
    	use plaid copy stream
  -profile string
    	write cpu profile to specified file
  -progress
    	Show progress bars
  -readonly
    	Only read a file for the benchmark
  -sendfile
    	Use the io.copyN impklementiontation
  -showmounts
    	Only Display Mounts
  -sizeMB int
    	Number MB generated per thread during benchmark
  -stream
    	Use the stream implementation
  -threads int
    	Number of concurrent threads default is core count * 2
  -verbose
    	Turn on Verbose logging
  -verify
    	re-read data to calculate hashes to verify data trasnfer integrity.
  -zeros
    	Benchmark Uses zeros instead of random data
```

## Spread Copy Engine
This engine divides the file evenly by each thread.  For example an 8 GiB file, with 8 threads, the First 
Thread will copy from 0-1GiB, and the second will start at 1GiB offset and read up to the 2GiB position, etc and 
each thread is also responsible for writing into that range on the destination.  This avoids doing a memory copy 
from the reader to the writer.  -sendfile attempts to use linux sendfile syscall, have not yet benchmarked to 
cofirm if this speeds it up on linux.   

Example streaming commands:
This will copy bigfile from an NFS mount to a local file using multiple streams.  By default 2 streams per core.
```
#./fbcp /mnt/nfs1/bigfile.txt /localfs/bigfile.txt
```

## Streaming Engine
If using a pipe for input or output you can't seek to the middle of the file as that is inherintly incompatible with a stream of data.  In this engine it uses a producer / consumer model in golang.   If the source is a stream the stream is just read directly.  However, if the source is a file a thread pool is created with a dispatcher that reads in chunks from the source. What ever thread is availble will pick up the next chunk.  Think of it as essentially a read ahead buffer.  The destination, if it is a stream, since it could receive bytes out of order it must buffer up to the thread count in order to write the destination stream in order.   If the destination is a file local or NFS.  A thread pool is created and as chunks come in they are distributed to the next available writer.  The destination file/NFS could receive chunks out of order however, they are written to their correct offset.  Also, todo: benchmark to find if this speeds things up at all or not.

Example streaming commands:
This will stream bigfile (from either the local FS, or connect directly to an NFS server) into wc
```
#./fbcp /mnt/nfs1/bigfile.txt | wc -l
```

Create a tar file with junk1 & junk2 inside
```
tar -cf - junk1 junk2 | ./fbcp junk.tar 
```

Extract the tar file into a tmp directory
```
# ./fbcp junk.tar | tar xvf - -C ./tmp/
```


## Hashing
Uses the XXH3 hash algorithm which is extremely fast and close to cryptographicly secure.  Split the file into even chunks (like the spread hash engine), hash each chunk and then create a hash of the hashes after all chunks finish.  This provides a single hash that represents the file.  Caution, the number of threads will change the hash!!   You must use the same number of threads to have the same hash.

If you specify -hash with a single file, it will hash that file.  

If you specify -hash with a file copy, it will hash the source file as it's read.

If you specify -verify with a file copy, it will copy the file hashing as it's reading and read back the copied file and verify the hashes.

Use hashing to verify the files we tar'ed and untar'ed in the previous example.

```
./fbcp/fbcp -hash junk1
INFO[0000] Found 8 cores                                
Read Throughput = 6546.840173 MiB/s
Spread Hash Threads: 16, Hash Node 1 of 1        Hash: abe64fcd3ab91ca1

./fbcp/fbcp_mac -hash tmp/junk1
INFO[0000] Found 8 cores                                
Read Throughput = 6557.580010 MiB/s
Spread Hash Threads: 16, Hash Node 1 of 1        Hash: abe64fcd3ab91ca1
```


You can validate a single thread hash of file using:
```
for x in `ls -1` ; do echo $x ; xxhsum -q -H3 $x | awk '{print $4}' | xxd -r -p | xxhsum -H3 ; done
```
It does a double hash on each file.  This should be accurate for all single threaded versions of the tool.

## Building
go build
env GOOS=linux GARCH=amd64  go build -o fbcp_linux


## Todo
 - Benchmark -sendifle on linux.
 - Benchmark streaming performance.
 - breakout benchmark into it's own binary
 - breakout hash into it's own binary.



