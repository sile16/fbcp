package main

import (
	"fmt"
	"io"
	"sync"

	log "github.com/sirupsen/logrus"
)

func NewNFSStream(src_ff *FlexFile, dst_ff *FlexFile, concurrency int ) (*NFSInfo, error) {

	n := &NFSInfo{ 
		src_ff: src_ff, dst_ff:dst_ff,
		concurrency: concurrency, filesWritten: 0}
	
	if !dst_ff.is_pipe {
		if  dst_ff.is_directory {
			log.Fatal("Destination file is a directory.")
		}

		dst_ff.Truncate(int64(src_ff.size))

		/* Todo: need to truncate the target file.
		if  dst_ff.exists {
			log.Fatal("Destination file already exists.")
		}*/
	} 

	n.sizeMB = uint64(1) * 1024 * 1024
	
	return n, nil
}

type ChannelMsg struct {
	offset uint64
	len    uint64
	data   *[]byte
}

func (n *NFSInfo) Stream() {
	pwg := &sync.WaitGroup{}
	cwg := &sync.WaitGroup{}

	var bufPool = sync.Pool{
		New: func() any {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			buf := make([]byte, n.sizeMB)
			return &buf
		},
	}

	ch := make(chan ChannelMsg )

	// spin up the consumers

	if n.dst_ff.is_pipe  {
		log.Debug("Starting Pipe consumer")
		cwg.Add(1)
		go n.PipeConsumer(ch, &bufPool, cwg)
	} else { 
		log.Debug("Starting NFS consumer")
		cwg.Add(n.concurrency)
		for i := 0; i < n.concurrency ; i++ {
			//fname := generateTestFilename(n.uniqueId, i)
			go n.NFSConsumer(ch, &bufPool, cwg)
		}
	}

	// spin up the producers
	if n.src_ff.is_pipe {
		log.Debug("Starting Pipe Producer")
		pwg.Add(1)
		go n.PipeProducer(ch, pwg, &bufPool)
	} else {
		log.Debug("Starting NFS Producer")
		dispatch := make(chan ChannelMsg)
		
		pwg.Add(n.concurrency)
		for i := 0; i < n.concurrency ; i++{
			go n.NFSProducer(dispatch, ch, pwg, &bufPool)
		}

		offset := uint64(0)

		for {
			bytes_to_read := n.sizeMB
			if offset + n.sizeMB > n.src_ff.size {
				bytes_to_read = n.src_ff.size - offset
			}
			dispatch <- ChannelMsg{offset: uint64(offset), len: bytes_to_read}
			offset += bytes_to_read
			if offset == n.src_ff.size {
				break
			}
		}
		close(dispatch)
	}

	pwg.Wait()
	close(ch)
	cwg.Wait()
}

func (n *NFSInfo) NFSProducer(dispatch <-chan ChannelMsg, ch chan<- ChannelMsg, pwg *sync.WaitGroup, pool *sync.Pool) {
	defer pwg.Done()

	nfs_f, err := n.src_ff.Open()
	if err != nil{
		fmt.Printf("error openeing source file %s ", n.src_ff.file_name)
		panic(err)
	}
	
	for msg := range dispatch {
		nfs_f.Seek(int64(msg.offset), io.SeekStart)

		n.mu.Lock()
		buf := pool.Get().(*[]byte)
		n.mu.Unlock()
		//buf := make([]byte, n.sizeMB)
		bytes_read := uint64(0)
		

		for {
			if bytes_read == msg.len {
				break
			}
			bytes_to_read := msg.len - bytes_read
			n_bytes, err := nfs_f.Read((*buf)[bytes_read:bytes_read+bytes_to_read])
			bytes_read += uint64(n_bytes)

			if err != nil {
				if err == io.EOF && n_bytes > 0 {
					if bytes_read == msg.len {
						log.Debugf("End of file error but with expected bytes , %s\n", err)
						break
					} else {
						log.Fatalf("Unexpected End of file error but , %s\n", err)
					}
				} else if err == io.EOF {
					log.Fatalf("Unexpected end of file, %s\n", err)
					break
				} else {
					log.Fatalf("Unknown error reading from file, %s\n", err)
				}
			}
			
		}
		ch <- ChannelMsg{offset: msg.offset, len: uint64(bytes_read), data: buf}
	}
}

func (n *NFSInfo) NFSConsumer(ch <-chan ChannelMsg, pool *sync.Pool, cwg *sync.WaitGroup) {
	defer cwg.Done()

	nfs_f, err := n.dst_ff.Open()
	if err != nil{
		panic(err)
	}
	defer nfs_f.Close()

	for msg := range ch {
		nfs_f.Seek(int64(msg.offset), io.SeekStart)
		bytes_written := uint64(0)

		for{
			bytes_to_write := msg.len - uint64(bytes_written)
			n_bytes, err := nfs_f.Write((*msg.data)[bytes_written:bytes_written+bytes_to_write])
			if err != nil {
				panic(err)
			}
			n.mu.Lock()
			pool.Put(msg.data)
			n.mu.Unlock()

			bytes_written += uint64(n_bytes)

			if bytes_written == msg.len {
				break
			}
		}
		
	}
}

func (n *NFSInfo) PipeProducer(ch chan<- ChannelMsg, pwg *sync.WaitGroup, pool *sync.Pool) {
	defer pwg.Done()

	//reader = bufio.NewReader(os.Stdin)
	reader, err := n.src_ff.Open()

	if err != nil {
		log.Fatal("Could not open source file with Pipe Producer")
	}

	var bytes_read uint64 = 0
	for {
		n.mu.Lock()
		buf := pool.Get().(*[]byte)
		n.mu.Unlock()
		//buf := make([]byte, n.sizeMB)
		n_bytes, err := reader.Read(*buf)
		
		if n_bytes > 0 {
			ch <- ChannelMsg{offset: bytes_read, len: uint64(n_bytes), data: buf}
			bytes_read += uint64(n_bytes)
		}

		if err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
	}
}

func (n *NFSInfo) PipeConsumer(ch <-chan ChannelMsg, pool *sync.Pool, cwg *sync.WaitGroup) {
	//reader = bufio.NewReader(os.Stdin)
	defer cwg.Done()

	writer, err := n.dst_ff.Open()
	if err != nil {
		log.Fatal("Pipe Consumer not able to open output file.")
	}
	defer writer.Close()


	items := make(map[uint64] ChannelMsg)
	//msg_count := 0

	var offset uint64 = 0
	for msg := range ch {
		items[msg.offset] = msg

		if len(items) > 10{
			log.Debugf("Pipe Consumer item_count: %d offset: %d  /n",len(items), offset)
		}

		for {
			//keep looping through our stored messages if we have the next offset already
			curr_msg, ok := items[offset]
			if ok {
				// For each message we need to keep writing until full message is send
				n_bytes_written := 0
				for {
					n_bytes, err := writer.Write((*curr_msg.data)[n_bytes_written:curr_msg.len])
					if err != nil {
						log.Fatal(err)
					}
					n_bytes_written += n_bytes
					if n_bytes == int(curr_msg.len){
						break
					}
				}

				n.mu.Lock()
				pool.Put(curr_msg.data)
				n.mu.Unlock()

				delete(items, offset)
				offset += uint64(curr_msg.len)
			} else {
				// no more stored need to go back to the channel for next message
				break
			}
		}
	}
}