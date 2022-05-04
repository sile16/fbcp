package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"io"
	"path/filepath"
	"errors"

	"github.com/joshuarobinson/go-nfs-client/nfs"
	"github.com/joshuarobinson/go-nfs-client/nfs/rpc"
)

type NFSInfo struct {
	concurrency     int
	hashes          [][]byte

	sizeMB          uint64
	nodeOffset      uint64
	nodeSize        uint64

	wg                        sync.WaitGroup
	mu                        sync.Mutex
	atm_finished              int32
	atm_counter_bytes_written uint64
	atm_counter_bytes_read    uint64
	filesWritten              int
	authUnix                  *rpc.AuthUnix
	src_ff                     *FlexFile
	dst_ff                     *FlexFile
}


type FlexFile struct {
	
	nfs_host  string
	export  string
	file_name string
	file_path string
	file_full_path string
	exists bool
	is_nfs bool
	size uint64
	is_directory bool
	is_pipe bool
	pipe *os.File

}

type ReadWriteSeekerCloser interface {
    io.Reader
    io.Writer
    io.Seeker
	io.Closer
}

func NewFlexFilePipe(*os.File) (*FlexFile, error) {
	ff := FlexFile{size: 0, exists: false, 
		is_directory: false, 
		is_pipe: false, is_nfs: false}

	return &ff, nil
}

func NewFlexFile(file_path string ) (*FlexFile, error){
	ff := FlexFile{size: 0, exists: false, is_directory: false, is_pipe: false}
	
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

		fsinfo, _ , err := target.Lookup(ff.file_name)
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
		}
		//todo: get mode bits here

		return &ff, nil
	}
}

func (ff *FlexFile) Open() (ReadWriteSeekerCloser, error) {
	// Open the File
	if ff.is_pipe {
		return ff.pipe, nil
	} else if ff.is_nfs {
		mount_dst, err := nfs.DialMount(ff.nfs_host, true)
		if err != nil {
			fmt.Println("Portmapper failed.")
			fmt.Println(err)
			return nil, err
		}
		//close todo: handle in FF

		hostname := getShortHostname()
		user_id := os.Getuid()
		group_id := os.Getgid()

		auth := rpc.NewAuthUnix(hostname, uint32(user_id), uint32(group_id))
		
		target_dst, err := mount_dst.Mount(ff.export, auth.Auth(), true)
		if err != nil {
			fmt.Println("Unable to mount.")
			fmt.Println(err)
			mount_dst.Close()
			return nil, err
		}
		//close todo: handle in FF

		var f_dst *nfs.File
		f_dst, err = target_dst.OpenFile(ff.file_name, os.FileMode(int(0644)))
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", ff.file_name)
			fmt.Println(err)
			target_dst.Close()
			mount_dst.Close()
			return nil, err
		}
		return f_dst, nil

	} else {
		var f_dst *os.File
		var err error
		/*if ff.exists{
			f_dst, err = os.Open(ff.file_full_path)
		} else {
			f_dst, err = os.Create(ff.file_full_path)
		}*/
		f_dst, err = os.OpenFile(ff.file_full_path, os.O_RDWR | os.O_CREATE, 0644)
		
		if err != nil {
			fmt.Printf("OpenFile %s failed\n", ff.file_full_path)
			fmt.Println(err)
			return nil, err
		}
		return  f_dst, nil
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
