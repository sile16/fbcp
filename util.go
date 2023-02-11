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
	hashes       [][]byte
	thread_bytes []int

	nodeOffset int
	nodeSize   int
	offset     int
	sizeMB     int

	wg                        sync.WaitGroup
	atm_finished              int64
	atm_counter_bytes_written int64
	atm_counter_bytes_read    int64
	filesWritten              int
	src_ff                    *FlexFile
	dst_ff                    *FlexFile

	c *Fbcp_config
}

const copy_buffer_size = 8 * 1024 * 1024

func (ff *FlexFileHandle) ReadFrom(r io.Reader) (n int64, err error) {
	if ff.ff.is_nfs {
		return ff.nfs_file.ReadFrom(r)
	} else if ff.ff.is_pipe {
		return ff.pipe_file.ReadFrom(r)
	} else if ff.ff.is_os_file {
		return ff.os_file.ReadFrom(r)
	} else {
		return 0, errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) WriteTo(w io.Writer) (n int64, err error) {
	if ff.ff.is_nfs {
		return ff.nfs_file.WriteTo(w)
	} else if ff.ff.is_pipe {
		return io.Copy(w, ff.pipe_file)
	} else if ff.ff.is_os_file {
		return io.CopyBuffer(w, ff.os_file, make([]byte, copy_buffer_size))
	} else {
		return 0, errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) Read(p []byte) (int, error) {
	//
	if ff.ff.is_nfs {
		return ff.nfs_file.Read(p)
	} else if ff.ff.is_pipe {
		return ff.pipe_file.Read(p)
	} else if ff.ff.is_os_file {
		return ff.os_file.Read(p)
	} else {
		return 0, errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) Write(p []byte) (int, error) {
	if ff.ff.is_nfs {
		return ff.nfs_file.Write(p)
	} else if ff.ff.is_pipe {
		return ff.pipe_file.Write(p)
	} else if ff.ff.is_os_file {
		return ff.os_file.Write(p)
	} else {
		return 0, errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) Close() error {
	if ff.ff.is_nfs {
		return ff.nfs_file.Close()
	} else if ff.ff.is_pipe {
		return ff.pipe_file.Close()
	} else if ff.ff.is_os_file {
		return ff.os_file.Close()
	} else {
		return errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) Seek(offset int64, whence int) (int64, error) {
	if ff.ff.is_nfs {
		return ff.nfs_file.Seek(offset, whence)
	} else if ff.ff.is_pipe {
		return ff.pipe_file.Seek(offset, whence)
	} else if ff.ff.is_os_file {
		return ff.os_file.Seek(offset, whence)
	} else {
		return 0, errors.New("unsupported file type")
	}
}

func (ff *FlexFileHandle) SetMaxReadSize(size uint32) {
	if ff.ff.is_nfs {
		ff.nfs_file.SetMaxReadSize(size)
	}
}

func (ff *FlexFileHandle) SetMaxWriteSize(size uint32) {
	if ff.ff.is_nfs {
		ff.nfs_file.SetMaxWriteSize(size)
	}
}

func (ff *FlexFileHandle) SetIODepth(depth int) {
	if ff.ff.is_nfs {
		ff.nfs_file.SetIODepth(depth)
	}
}



///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////

type FlexFileHandle struct {
	os_file   *os.File
	nfs_file  *nfs.File
	pipe_file *os.File

	ff 		*FlexFile
}


type FlexFile struct {
	Nfs_host        string
	Export          string
	file_name       string
	File_path       string
	File_full_path  string
	direct_nfs_path string
	mount_info      *MountEntry

	pipe_file *os.File

	exists       bool
	is_nfs       bool
	Size         int
	is_directory bool
	is_pipe      bool
	is_os_file   bool
	pipe         *os.File
}

func NewFlexFilePipe(pipe *os.File) (*FlexFile, error) {
	ff := FlexFile{Size: 0, exists: false,
		is_directory: false,
		is_pipe:      true, is_nfs: false, pipe: pipe, is_os_file: false,
		file_name: pipe.Name(),
		pipe_file: pipe}

	return &ff, nil
}

func NewFlexFile(file_path string) (*FlexFile, error) {
	ff := FlexFile{Size: 0, exists: false, is_directory: false,
		is_pipe: false, is_os_file: false}

	if strings.Contains(file_path, ":") {
		ff.is_nfs = true

		tmp := strings.Split(file_path, ":")
		ff.Nfs_host = tmp[0]
		ff.Export = filepath.Dir(tmp[1])
		ff.file_name = filepath.Base(tmp[1])
		ff.File_full_path = file_path

		// Try and mount to verify
		mount, err := nfs.DialMount(ff.Nfs_host, true)
		if err != nil {
			err := errors.New("[error] FlexFile Unable to dial mount service, ")
			return &ff, err
		}
		defer mount.Close()

		hostname := getShortHostname()
		user_id := os.Getuid()
		group_id := os.Getgid()

		authUnix := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))

		target, err := mount.Mount(ff.Export, authUnix.Auth(), true)
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
		ff.Size = int(fsinfo.Size())
		return &ff, nil

		//fsinfo := f.FSInfo()

	} else {
		ff.is_nfs = false
		ff.File_path = filepath.Dir(file_path)
		ff.file_name = filepath.Base(file_path)
		ff.File_full_path, _ = filepath.Abs(file_path)

		file_info, err := os.Stat(file_path)
		if err != nil {
			return &ff, nil
		}
		ff.Size = int(file_info.Size())
		ff.exists = true

		if file_info.IsDir() {
			ff.is_directory = true
		} else {
			ff.is_os_file = true
		}

		ff.direct_nfs_path, ff.mount_info = GetNFSPathFromLocal(ff.File_full_path)

		//todo: get mode bits here

		return &ff, nil
	}
}

func (ff *FlexFile) Truncate(size int64) error {
	if ff.pipe != nil {
		return nil
	} else if ff.is_nfs {

		mount_dst, err := nfs.DialMount(ff.Nfs_host, true)
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

		target_dst, err := mount_dst.Mount(ff.Export, auth.Auth(), true)
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
		return os.Truncate(ff.File_full_path, size)
	}

}

func (ff *FlexFile) Open() (*FlexFileHandle, error) {
	// Open the File
	if ff.pipe != nil {
		ff.is_pipe = true
		ff.is_nfs = false
		ff.is_os_file = false
		ff.is_directory = false
		ff.exists = true

		//pipe is a stream that's already open so we don't need to open it again
		return &FlexFileHandle{
			pipe_file: ff.pipe,
			ff:   ff,
		}, nil

	} else if ff.is_nfs {
		mount_dst, err := nfs.DialMount(ff.Nfs_host, true)

		if err != nil {
			log.Warnf("DialMount %s failed\n", ff.Nfs_host)
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)
			mount_dst, err = nfs.DialMount(ff.Nfs_host, true)
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

		target_dst, err := mount_dst.Mount(ff.Export, auth.Auth(), true)
		if err != nil {
			log.Warn("Unable to mount.")
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)
			target_dst, err = mount_dst.Mount(ff.Export, auth.Auth(), true)
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
			log.Warnf("OpenFile %s failed\n", ff.file_name)
			log.Warnf("%s", err)
			time.Sleep(time.Millisecond * 10)

			f_dst, err = target_dst.OpenFile(ff.file_name, os.FileMode(int(0644)))
			//Built in retry because requests can be sent too fast for the server.
			if err != nil {
				log.Warnf("File Open retry failed.")
				target_dst.Close()
				mount_dst.Close()
				return nil, err
			} else {
				log.Warnf("File Open Retry succeeded")
			}
		} else {
			log.Debugf("File Open succeeded")
		}

		return &FlexFileHandle{
			nfs_file: f_dst,
			ff:   ff,
		}, nil

	} else {
		var f_dst *os.File
		var err error

		//todo: create with correct mode mits based on source file.
		f_dst, err = os.OpenFile(ff.File_full_path, os.O_RDWR|os.O_CREATE, 0644)

		if err != nil {
			fmt.Printf("OpenFile %s failed\n", ff.File_full_path)
			fmt.Println(err)
			return nil, err
		} 
		return &FlexFileHandle{
			os_file: f_dst,
			ff:   ff,
		}, nil
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

func getBytesPerThread(size int, nodes int, concurrency int) int {
	if size == 0 {
		return min_thread_size_bytes
	}

	// Round up to nearest multiple of 1MB per thread across all nodes
	// https://stackoverflow.com/questions/9303604/rounding-up-a-number-to-nearest-multiple-of-5

	OneMB_per_thread_per_node := 1024 * 1024 * nodes * concurrency

	// this is the way to round up to a multiple of OneMB_per_thread_per_node
	padded_size := (size + (OneMB_per_thread_per_node - 1)) / OneMB_per_thread_per_node * OneMB_per_thread_per_node

	bytes_per_thread := padded_size / ( nodes * concurrency )

	// Check to make sure we are at the minimum
	if bytes_per_thread < min_thread_size_bytes {
		bytes_per_thread = min_thread_size_bytes
	}

	return bytes_per_thread
}

func getThreadCount(size int, nodes int, bytes_per_thread int) int {

	if size == 0 {
		return 1
	}

	// total threads across all nodes
	threads := size / bytes_per_thread
	if threads == 0 {
		return 1
	}

	remainder_bytes := size - threads * bytes_per_thread
	if remainder_bytes > 0 {
		threads++
	}

	// Round thread count up to a mulitple of the node count.
	threads_per_node := (threads + (nodes - 1)) / nodes

	return threads_per_node
}

func NewFlexFileDirectNFS(ff *FlexFile) *FlexFile {

	//Todo: check is nconnect is avaialbe which is best perf?
	if ff.direct_nfs_path != "" {
		new_nfs_ff, err := NewFlexFile(ff.direct_nfs_path)
		if err != nil {
			log.Debugf("Error creating flex file for %s", ff.direct_nfs_path)
			log.Debug("Falling back to local path")
			return ff
		} else {
			log.Debug("Changing src to direct NFS")
			//todo: close old file
			//ff.Close()
			return new_nfs_ff
		}

	}
	return ff

}
