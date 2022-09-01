package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/sile16/go-nfs-client/nfs"
	"github.com/sile16/go-nfs-client/nfs/rpc"
)

type NFSInfo struct {
	concurrency  int
	hashes       [][]byte
	thread_bytes []uint64

	sizeMB     uint64
	nodeOffset uint64
	nodeSize   uint64
	hash       bool
	zeros      bool
	copyv2     bool
	plaid      bool
	progress   bool

	wg                        sync.WaitGroup
	atm_finished              int32
	atm_counter_bytes_written uint64
	atm_counter_bytes_read    uint64
	filesWritten              int
	src_ff                    *FlexFile
	dst_ff                    *FlexFile
}

type ReadWriteSeekerCloser interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

type ReadWriteSeekerCloserReaderFrom interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	io.ReaderFrom
}

///////////////////////////////////////////////////////////////////
// Adding the Readfrom function, so that I can use os.file readfrom.
// which on linux implements the sendfile function for high perf
// so, all NFS calls, even if not REadFrom go through this wrapper
// I wouldn't think it makes a difference
type ReadFromFileWrapper struct {
	fh ReadWriteSeekerCloser
}

func NewReadFromFileWrapper(nfs_file ReadWriteSeekerCloser) *ReadFromFileWrapper {
	return &ReadFromFileWrapper{
		fh: nfs_file,
	}
}

func (fw *ReadFromFileWrapper) ReadFrom(r io.Reader) (n int64, err error) {
	return io.Copy(fw.fh, r)
}

func (fw *ReadFromFileWrapper) Read(p []byte) (int, error) {
	return fw.fh.Read(p)
}

func (fw *ReadFromFileWrapper) Write(p []byte) (int, error) {
	return fw.fh.Write(p)
}

func (fw *ReadFromFileWrapper) Close() error {
	return fw.fh.Close()
}

func (fw *ReadFromFileWrapper) Seek(offset int64, whence int) (int64, error) {
	return fw.fh.Seek(offset, whence)
}

///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////

type FlexFile struct {
	nfs_host       string
	export         string
	file_name      string
	file_path      string
	file_full_path string

	exists       bool
	is_nfs       bool
	size         uint64
	is_directory bool
	is_pipe      bool
	is_os_file   bool
	pipe         *os.File
}

func NewFlexFilePipe(pipe *os.File) (*FlexFile, error) {
	ff := FlexFile{size: 0, exists: false,
		is_directory: false,
		is_pipe:      true, is_nfs: false, pipe: pipe, is_os_file: false,
		file_name: pipe.Name()}

	return &ff, nil
}

func NewFlexFile(file_path string) (*FlexFile, error) {
	ff := FlexFile{size: 0, exists: false, is_directory: false,
		is_pipe: false, is_os_file: false}

	if strings.Contains(file_path, ":") {
		ff.is_nfs = true

		tmp := strings.Split(file_path, ":")
		ff.nfs_host = tmp[0]
		ff.export = filepath.Dir(tmp[1])
		ff.file_name = filepath.Base(tmp[1])
		ff.file_full_path = file_path

		// Try and mount to verify
		mount, err := nfs.DialMount(ff.nfs_host, true)
		if err != nil {
			err := errors.New("[error] FlexFile Unable to dial mount service, ")
			return &ff, err
		}
		defer mount.Close()

		hostname := getShortHostname()
		user_id := os.Getuid()
		group_id := os.Getgid()

		authUnix := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

		target, err := mount.Mount(ff.export, authUnix.Auth(), true)
		if err != nil {
			err := errors.New("[error] FlexFile Unable to mount export, ")
			return &ff, err
		}
		defer target.Close()

		fsinfo, _, err := target.Lookup(ff.file_name)
		if err != nil {
			// File does'nt exist ?
			ff.exists = false

			//fmt.Println(err)
			return &ff, nil
		}
		ff.exists = true

		if fsinfo.IsDir() {
			ff.is_directory = true
		}
		ff.size = uint64(fsinfo.Size())
		return &ff, nil

		//fsinfo := f.FSInfo()

	} else {
		ff.is_nfs = false
		ff.file_path = filepath.Dir(file_path)
		ff.file_name = filepath.Base(file_path)
		ff.file_full_path, _ = filepath.Abs(file_path)

		file_info, err := os.Stat(file_path)
		if err != nil {
			return &ff, nil
		}
		ff.size = uint64(file_info.Size())
		ff.exists = true

		if file_info.IsDir() {
			ff.is_directory = true
		} else {
			ff.is_os_file = true
		}

		//todo: get mode bits here

		return &ff, nil
	}
}

func (ff *FlexFile) Truncate(size int64) error {
	if ff.pipe != nil {
		return nil
	} else if ff.is_nfs {

		mount_dst, err := nfs.DialMount(ff.nfs_host, true)
		if err != nil {
			fmt.Println("Portmapper failed.")
			fmt.Println(err)
			return err
		}
		defer mount_dst.Close()

		hostname := getShortHostname()
		user_id := os.Getuid()
		group_id := os.Getgid()

		auth := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

		target_dst, err := mount_dst.Mount(ff.export, auth.Auth(), true)
		if err != nil {
			fmt.Println("Unable to mount.")
			fmt.Println(err)
			mount_dst.Close()
			return err
		}
		defer target_dst.Close()

		log.Debugf("Truncating the NFS file, %s, to size: %d", ff.file_name, size)
		_, err = target_dst.CreateTruncate(ff.file_name, os.FileMode(int(0644)), uint64(size))

		if err != nil {
			log.Errorf("Error tyring to truncate file")
		}
		return nil

	} else {
		return os.Truncate(ff.file_full_path, size)
	}

}

func (ff *FlexFile) Open() (ReadWriteSeekerCloserReaderFrom, error) {
	// Open the File
	if ff.pipe != nil {
		return ff.pipe, nil

	} else if ff.is_nfs {
		mount_dst, err := nfs.DialMount(ff.nfs_host, true)
		if err != nil {
			log.Warnf("DialMount %s failed\n", ff.nfs_host)
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)
			mount_dst, err = nfs.DialMount(ff.nfs_host, true)
			if err != nil {
				log.Warnf("Dial Mount retry failed.")
				return nil, err
			} else {
				log.Warnf("Mount Retry succeeded")
			}
		}
		//close todo: handle in FF

		hostname := getShortHostname()
		user_id := os.Getuid()
		group_id := os.Getgid()

		auth := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

		target_dst, err := mount_dst.Mount(ff.export, auth.Auth(), true)
		if err != nil {
			log.Warn("Unable to mount.")
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)
			target_dst, err = mount_dst.Mount(ff.export, auth.Auth(), true)
			if err != nil {
				log.Warnf("Mount retry failed.")
				mount_dst.Close()
				return nil, err
			} else {
				log.Warnf("Mount Retry succeeded")
			}
		}
		//close todo: handle in FF

		var f_dst *nfs.File
		f_dst, err = target_dst.OpenFile(ff.file_name, os.FileMode(int(0644)))
		if err != nil {
			log.Warn("OpenFile %s failed\n", ff.file_name)
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)
			f_dst, err = target_dst.OpenFile(ff.file_name, os.FileMode(int(0644)))
			if err != nil {
				log.Warnf("File Open retry failed.")
				target_dst.Close()
				mount_dst.Close()
				return nil, err
			} else {
				log.Warnf("File Open Retry succeeded")
			}
		}
		f_dst_wrapper := NewReadFromFileWrapper(f_dst)
		return f_dst_wrapper, nil

	} else {
		var f_dst *os.File
		var err error

		//todo: create with correct mode mits based on source file.
		f_dst, err = os.OpenFile(ff.file_full_path, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Printf("OpenFile %s failed\n", ff.file_full_path)
			fmt.Println(err)
			return nil, err
		}
		return f_dst, nil
	}

}

func (ff *FlexFile) ReadFrom(r io.Reader) (n int64, err error) {

	if ff.is_pipe {
		return ff.pipe.ReadFrom(r)

	} else if ff.is_nfs {
		//todo implement readfrom in NFS client.
		//return 0, errors.New("ReadFrom Not implemented for nfs")
		return
		
	} else {
		// we are a regular file at this point.
		var f_dst *os.File
		var err error

		f_dst, err = os.OpenFile(ff.file_full_path, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Printf("OpenFile %s failed\n", ff.file_full_path)
			fmt.Println(err)
			return 0, err
		}

		return f_dst.ReadFrom(r)
	}
}

func ByteRateSI(b float64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%.1f B/s", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB/s", b/float64(div), "kMGTPE"[exp])
}

func getShortHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Warning, null hostname.")
		hostname = "null"
	}
	// Use only the short hostname because dots are invalid in filesystem names.
	hostname = strings.Split(hostname, ".")[0]
	return strings.ToLower(hostname)
}

func getThreadCount(size uint64, nodes uint64, bytes_per_thread uint64) uint64 {

	if size == 0 {
		return 1
	}

	// total threads across all nodes
	threads := size / bytes_per_thread
	remainder_bytes := threads*bytes_per_thread - size
	if remainder_bytes > 0 {
		threads++
	}

	// Round thread count up to a mulitple of the node count.
	threads_per_node := (threads + (nodes - 1)) / nodes

	return threads_per_node
}

func getBytesPerThread(size uint64, nodes int, concurrency int) uint64 {
	if size == 0 {
		return min_thread_size
	}

	// Round up to nearest multiple of 1MB per thread across all nodes
	// https://stackoverflow.com/questions/9303604/rounding-up-a-number-to-nearest-multiple-of-5

	OneMB_per_thread_per_node := 1024 * 1024 * uint64(nodes*concurrency)

	// this is the way to round up to a multiple of OneMB_per_thread_per_node
	padded_size := (size + (OneMB_per_thread_per_node - 1)) / OneMB_per_thread_per_node * OneMB_per_thread_per_node

	bytes_per_thread := padded_size / uint64(nodes*concurrency)

	// Check to make sure we are at the minimum
	if bytes_per_thread < min_thread_size {
		bytes_per_thread = min_thread_size
	}

	return bytes_per_thread
}
