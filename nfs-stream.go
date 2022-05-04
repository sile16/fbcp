package main

import (
	"fmt"
	"io"
	"os"
	"sync"
)

func NewNFSStream(src_ff *FlexFile, dst_ff *FlexFile, concurrency int ) (*NFSInfo, error) {

	n := &NFSInfo{ 
		src_ff: src_ff, dst_ff:dst_ff,
		concurrency: concurrency, filesWritten: 0}

	/*
	var err error
	
	if !dst_ff.is_pipe {
		if  dst_ff.is_directory {
			fmt.Println("Destination file is a directory.")
			os.Exit(1)
		}

		if  dst_ff.exists {
			fmt.Println("Destination file already exists.")
			os.Exit(1)
		}
	} */

	n.sizeMB = uint64(1) * 1024 * 1024
	
	return n, nil
}

type ChannelMsg struct {
	offset uint64
	len    uint64
	data   []byte
}

func (n *NFSInfo) Stream() {
	pwg := &sync.WaitGroup{}
	cwg := &sync.WaitGroup{}

	
	var bufPool = sync.Pool{
		New: func() any {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			return make([]byte, n.sizeMB)
		},
	}

	ch := make(chan ChannelMsg )

	// spin up the consumers

	if n.dst_ff.is_pipe  {
		cwg.Add(1)
		go n.PipeConsumer(ch, &bufPool, cwg)
	} else { 
		cwg.Add(n.concurrency)
		for i := 0; i < n.concurrency ; i++ {
			//fname := generateTestFilename(n.uniqueId, i)
			go n.NFSConsumer(ch, &bufPool, cwg)
		}
	}

	// spin up the producers
	if n.src_ff.is_pipe {
		pwg.Add(1)
		go n.PipeProducer(ch, pwg, &bufPool)
	} else {
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

		//n.mu.Lock()
		//buf := pool.Get().([]byte)
		//n.mu.Unlock()
		buf := make([]byte, n.sizeMB)
		bytes_read := uint64(0)

		for {
			if bytes_read == msg.len {
				break
			}
			bytes_to_read := msg.len - bytes_read
			n_bytes, err := nfs_f.Read(buf[bytes_read:bytes_read+bytes_to_read])
			bytes_read += uint64(n_bytes)

			if err != nil {
				if err == io.EOF && n_bytes > 0 {
					break
				} else if err == io.EOF {
					break
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
			n_bytes, err := nfs_f.Write(msg.data[bytes_written:bytes_written+bytes_to_write])
			if err != nil {
				panic(err)
			}
			//n.mu.Lock()
			//pool.Put(&msg.data)
			//n.mu.Unlock()

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
	reader := io.Reader(os.Stdin)

	var bytes_read uint64 = 0
	for {
		//n.mu.Lock()
		//buf := pool.Get().([]byte)
		//n.mu.Unlock()
		buf := make([]byte, n.sizeMB)
		n_bytes, err := reader.Read(buf)
		
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

	writer, _ := n.dst_ff.Open()


	items := make(map[uint64] ChannelMsg)
	//msg_count := 0

	var offset uint64 = 0
	for msg := range ch {
		items[msg.offset] = msg

		curr_msg, ok := items[offset]
		if ok {
			n_bytes, err := writer.Write(curr_msg.data[0:curr_msg.len])
			if err != nil {
				panic(err)
			}
			n.mu.Lock()
			pool.Put(&curr_msg.data)
			n.mu.Unlock()

			if uint64(n_bytes) != msg.len {
				panic("Pipe consumer bytes written do not match bytes sent")
			}
			offset += uint64(n_bytes)
			delete(items, offset)
		}
	}
}