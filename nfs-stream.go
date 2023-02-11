package main

import (
	"fmt"
	"io"
	"sync"

	log "github.com/sirupsen/logrus"
)

func NewNFSStream(src_ff *FlexFile, dst_ff *FlexFile, c *Fbcp_config) (*NFSInfo, error) {

	n := &NFSInfo{
		src_ff: src_ff, dst_ff: dst_ff,
		c: c, filesWritten: 0}

	if dst_ff.is_directory {
		log.Fatal("Destination file is a directory.")
	}

	if dst_ff.pipe == nil {
		log.Debugf("Truncating Target filename: %s", dst_ff.file_name)
		dst_ff.Truncate(int64(src_ff.Size))
	}

	n.sizeMB = n.c.sizeMB * 1024 * 1024

	return n, nil
}

type ChannelMsg struct {
	offset int
	len    int
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

	ch := make(chan ChannelMsg)

	// spin up the consumers

	if n.dst_ff.is_pipe {
		log.Debug("Starting Pipe consumer")
		cwg.Add(1)
		go n.PipeConsumer(ch, &bufPool, cwg)
	} else {
		log.Debug("Starting NFS consumer")
		cwg.Add(n.c.threads)
		for i := 0; i < n.c.threads; i++ {
			go n.NFSConsumer(ch, &bufPool, cwg)
		}
	}

	// spin up the producers
	if n.src_ff.is_pipe {
		log.Debug("Starting Pipe Producer")
		if n.c.plaid {
			log.Warn("Ignoring Plaid, because producer is a pipe.")
		}
		pwg.Add(1)
		go n.PipeProducer(ch, pwg, &bufPool)
	} else {
		log.Debug("Starting NFS Producer")
		dispatch := make(chan ChannelMsg)

		pwg.Add(n.c.threads)
		for i := 0; i < n.c.threads; i++ {
			go n.NFSProducer(dispatch, ch, pwg, &bufPool)
		}

		offset := 0

		if n.c.plaid && n.dst_ff.is_pipe {
			log.Warn("Ignoring Plaid Producer because output is a pipe.")
		}

		if n.c.plaid && !n.dst_ff.is_pipe {
			// dispatch reads in a plaid manner
			// would use up a ton of memmory if consumer is
			log.Debugf("Starting a Plaid NFS Streamer")
			min_thread_size := 32 * 1024 * 1024

			bytes_per_thread := n.src_ff.Size / n.c.threads
			//remainder_per_thread :=  bytes_per_thread % min_thread_size
			//bytes_per_thread += min_thread_size - remainder_per_thread

			if bytes_per_thread < min_thread_size {
				bytes_per_thread = min_thread_size
			}

			per_thread_offset := 0
			for {
				for x := 0; x < n.c.threads; x++ {
					thread_offset := bytes_per_thread * x + per_thread_offset
					len := n.sizeMB
					// we expect the last thread in the loop to get generated
					// messages that may exceed eof so we need to check for it.
					if thread_offset >= n.src_ff.Size {
						continue
					} else if thread_offset+n.sizeMB > n.src_ff.Size {
						len = n.src_ff.Size - thread_offset
					}
					dispatch <- ChannelMsg{offset: thread_offset, len: len}
				}
				per_thread_offset += n.sizeMB
				if per_thread_offset >= bytes_per_thread {
					break
				}
			}
		} else {
			for {
				bytes_to_read := n.sizeMB
				if offset + n.sizeMB > n.src_ff.Size {
					bytes_to_read = n.src_ff.Size - offset
				}
				dispatch <- ChannelMsg{offset: offset, len: bytes_to_read}
				offset += bytes_to_read
				if offset == n.src_ff.Size {
					break
				}
			}
		}
		close(dispatch)
	}
	// wait for producers
	pwg.Wait()
	// Close the channel indicating no new messages
	close(ch)
	// Wait for all the consumers to finish
	cwg.Wait()
}

func (n *NFSInfo) NFSProducer(dispatch <-chan ChannelMsg, ch chan<- ChannelMsg, pwg *sync.WaitGroup, pool *sync.Pool) {
	defer pwg.Done()

	nfs_f, err := n.src_ff.Open()
	if err != nil {
		fmt.Printf("error openeing source file %s ", n.src_ff.file_name)
		panic(err)
	}

	for msg := range dispatch {
		nfs_f.Seek(int64(msg.offset), io.SeekStart)

		//n.mu.Lock()
		buf := pool.Get().(*[]byte)
		//n.mu.Unlock()
		//buf := make([]byte, n.sizeMB)
		bytes_read := 0

		for {
			if bytes_read == msg.len {
				break
			}
			bytes_to_read := msg.len - bytes_read
			n_bytes, err := nfs_f.Read((*buf)[bytes_read : bytes_read+bytes_to_read])
			bytes_read += n_bytes

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
		ch <- ChannelMsg{offset: msg.offset, len: bytes_read, data: buf}
	}
}

func (n *NFSInfo) NFSConsumer(ch <-chan ChannelMsg, pool *sync.Pool, cwg *sync.WaitGroup) {
	defer cwg.Done()

	nfs_f, err := n.dst_ff.Open()
	if err != nil {
		panic(err)
	}
	defer nfs_f.Close()

	for msg := range ch {
		nfs_f.Seek(int64(msg.offset), io.SeekStart)
		bytes_written := 0

		for {
			bytes_to_write := msg.len - bytes_written
			n_bytes, err := nfs_f.Write((*msg.data)[bytes_written : bytes_written+bytes_to_write])
			if err != nil {
				panic(err)
			}
			//n.mu.Lock()
			pool.Put(msg.data)
			//n.mu.Unlock()

			bytes_written += n_bytes

			if bytes_written == msg.len {
				break
			}
		}

	}
}

func (n *NFSInfo) PipeProducer(ch chan<- ChannelMsg, pwg *sync.WaitGroup, pool *sync.Pool) {
	defer pwg.Done()

	reader, err := n.src_ff.Open()

	if err != nil {
		log.Fatal("Could not open source file with Pipe Producer")
	}

	bytes_read := 0
	for {
		//n.mu.Lock()
		buf := pool.Get().(*[]byte)
		//n.mu.Unlock()
		//buf := make([]byte, n.sizeMB)
		n_bytes, err := reader.Read(*buf)

		if n_bytes > 0 {
			ch <- ChannelMsg{offset: bytes_read, len: n_bytes, data: buf}
			bytes_read += n_bytes
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

	items := make(map[int]ChannelMsg)
	//msg_count := 0

	var offset int = 0
	for msg := range ch {
		items[msg.offset] = msg

		if len(items) > 10 {
			log.Debugf("Pipe Consumer item_count: %d offset: %d  /n", len(items), offset)
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
					if n_bytes == int(curr_msg.len) {
						break
					}
				}

				//n.mu.Lock()
				pool.Put(curr_msg.data)
				//n.mu.Unlock()

				delete(items, offset)
				offset += curr_msg.len
			} else {
				// no more stored need to go back to the channel for next message
				break
			}
		}
	}
}
