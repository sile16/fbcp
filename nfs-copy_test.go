package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestNFSCopyHash(tb *testing.T) {
	setupSuite(tb)
	log.SetLevel(log.DebugLevel)

	for _, tc := range hash_tests {
		name := tc.name + "__" + strconv.FormatInt(int64(tc.threads), 10) + " Threads"

		tb.Run(name, func(t *testing.T) {

			file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))

			src_ff, _ := NewFlexFile(file_path)
			dst_ff, _ := NewFlexFile("/dev/null")

			nfscopy, _ := NewNFSCopy(src_ff, dst_ff, tc.threads, 1, 0, 0, true, false, false)
			_, hash := nfscopy.SpreadCopy()

			hash_string := fmt.Sprintf("%x", hash)

			if hash_string != tc.expected {
				t.Errorf("expected %s, got %s", tc.expected, hash_string)
			}
		})
	}
}

func TestNFSCopyValidate(tb *testing.T) {
	setupSuite(tb)
	//hash the source file with spreadhash
	//copy the file
	//hash the remote file wtih spreadhash

	for _, tc := range hash_tests[:len(hash_tests)/2] {

		thread_list := []int{1, 7, 16, 24, 32}
		copyv2_list := []bool{false, true}

		for thread := 0; thread < len(thread_list); thread++ {
			for copyv2 := 0; copyv2 < len(copyv2_list); copyv2++ {
				name := tc.name + "_CP_" + strconv.FormatInt(int64(thread_list[thread]), 10) + " Threads"
				if copyv2_list[copyv2] {
					name += "_copyv2"
				}

				file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))

				//hash the source file first out of the timed function
				src_ff_hash, _ := NewFlexFile(file_path)
				nfshash_src, _ := NewSpreadHash(src_ff_hash, thread_list[thread], 1, 0, 0, false)
				_, verify_hash := nfshash_src.SpreadHash()

				verify_hash_string := fmt.Sprintf("%x", verify_hash)

				src_ff, _ := NewFlexFile(file_path)
				dst_ff, _ := NewFlexFile("tempdir/dest_file")

				tb.Run(name, func(t *testing.T) {
					//copy the file
					nfscopy, _ := NewNFSCopy(src_ff, dst_ff, thread_list[thread], 1, 0, 0, true, copyv2_list[copyv2], false)
					_, copy_hash := nfscopy.SpreadCopy()

					if !copyv2_list[copyv2] {
						copy_hash_string := fmt.Sprintf("%x", copy_hash)
						if copy_hash_string != verify_hash_string {
							t.Errorf("Copied hash %s doesn't equal source file hash %s",
								copy_hash_string, verify_hash_string)
						}
					}

					dst_ff_hash, _ := NewFlexFile("tempdir/dest_file")
					nfshash, _ := NewSpreadHash(dst_ff_hash, thread_list[thread], 1, 0, 0, false)
					_, dst_hash := nfshash.SpreadHash()

					dst_hash_string := fmt.Sprintf("%x", dst_hash)
					if dst_hash_string != verify_hash_string {
						t.Errorf("Destination hash %s doesn't equal source file hash %s",
							dst_hash_string, verify_hash_string)
					}
				})

			}
		}
	}
}

func TestNFSRemoteCopyHashingValidate(tb *testing.T) {
	setupSuite(tb)

	//copy the file
	//copy the file back
	//double check hashes against local spreadhash.

	nfs_server := "127.0.0.1:/Volumes/RAMDisk/"

	local_hashes := make(map[string]string)

	thread_list := []int{1, 3, 7}
	// pre-calculate all hashes on the local files.
	for _, tc := range hash_tests[:len(hash_tests)/2] {

		for thread := 0; thread < len(thread_list); thread++ {

			local_file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))

			if local_file_path == "0" {
				log.Info("we have a 0 file.")
			}

			validation_hash_name := local_file_path + "_t" + strconv.FormatInt(int64(thread_list[thread]), 10)

			//hash the source file first out of the timed function
			src_ff_hash, _ := NewFlexFile(local_file_path)
			nfshash_src, _ := NewSpreadHash(src_ff_hash, thread_list[thread], 1, 0, 0, false)
			_, verify_hash := nfshash_src.SpreadHash()

			verify_hash_string := fmt.Sprintf("%x", verify_hash)
			local_hashes[validation_hash_name] = verify_hash_string
		}
	}

	for _, tc := range hash_tests[:len(hash_tests)/2] {

		copyv2_list := []bool{false}

		for thread := 0; thread < len(thread_list); thread++ {
			for copyv2 := 0; copyv2 < len(copyv2_list); copyv2++ {
				name := tc.name + "_CP_" + strconv.FormatInt(int64(thread_list[thread]), 10) + " Threads"
				if copyv2_list[copyv2] {
					name += "_copyv2"
				}

				local_file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))
				remote_file_path := nfs_server + strconv.FormatUint(tc.size, 10)

				src_ff, _ := NewFlexFile(local_file_path)
				dst_ff, _ := NewFlexFile(remote_file_path)

				validation_hash_name := local_file_path + "_t" + strconv.FormatInt(int64(thread_list[thread]), 10)
				validation_hash := local_hashes[validation_hash_name]

				tb.Run(name, func(t *testing.T) {
					//copy the file
					nfscopy, _ := NewNFSCopy(src_ff, dst_ff, thread_list[thread], 1, 0, 0, true, copyv2_list[copyv2], false)
					_, copy_hash := nfscopy.SpreadCopy()

					if !copyv2_list[copyv2] {
						copy_hash_string := fmt.Sprintf("%x", copy_hash)
						if copy_hash_string != validation_hash {
							t.Errorf("Copied hash %s doesn't equal source file hash %s",
								copy_hash_string, local_hashes[local_file_path])
						}
					}

					src_ff_copy_back, _ := NewFlexFile(remote_file_path)
					dst_ff_copy_back, _ := NewFlexFile("tempdir/dest_file")

					nfscopyback, _ := NewNFSCopy(src_ff_copy_back, dst_ff_copy_back, thread_list[thread], 1, 0, 0, true, copyv2_list[copyv2], false)
					_, dst_hash := nfscopyback.SpreadCopy()

					dst_hash_string := fmt.Sprintf("%x", dst_hash)
					if dst_hash_string != validation_hash {
						t.Errorf("Destination hash %s doesn't equal source file hash %s",
							dst_hash_string, local_hashes[local_file_path])
					}

				})

			}
		}
	}
}

func TestNFSRemoteCopyValidateWithSpreadHash(tb *testing.T) {
	setupSuite(tb)

	//hash locally
	//copy the file
	//hash remotely.

	nfs_server := "127.0.0.1:/Volumes/RAMDisk/"
	thread_list := []int{1, 3, 7}

	// pre-calculate all hashes on the local files.
	local_hashes := make(map[string]string)
	for _, tc := range hash_tests[:len(hash_tests)/2] {

		for thread := 0; thread < len(thread_list); thread++ {

			local_file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))

			if local_file_path == "0" {
				log.Info("we have a 0 file.")
			}

			validation_hash_name := local_file_path + "_t" + strconv.FormatInt(int64(thread_list[thread]), 10)

			//hash the source file first out of the timed function
			src_ff_hash, _ := NewFlexFile(local_file_path)
			nfshash_src, _ := NewSpreadHash(src_ff_hash, thread_list[thread], 1, 0, 0, false)
			_, verify_hash := nfshash_src.SpreadHash()

			verify_hash_string := fmt.Sprintf("%x", verify_hash)
			local_hashes[validation_hash_name] = verify_hash_string
		}
	}

	for _, tc := range hash_tests[:len(hash_tests)/2] {

		copyv2_list := []bool{false} //can't use copyv2 as it doesn't hash anything.

		for thread := 0; thread < len(thread_list); thread++ {
			for copyv2 := 0; copyv2 < len(copyv2_list); copyv2++ {
				name := tc.name + "_CP_" + strconv.FormatInt(int64(thread_list[thread]), 10) + " Threads"
				if copyv2_list[copyv2] {
					name += "_copyv2"
				}

				local_file_path := filepath.Join("tempdir", strconv.FormatUint(tc.size, 10))
				remote_file_path := nfs_server + strconv.FormatUint(tc.size, 10)

				validation_hash_name := local_file_path + "_t" + strconv.FormatInt(int64(thread_list[thread]), 10)
				validation_hash := local_hashes[validation_hash_name]

				src_ff, _ := NewFlexFile(local_file_path)
				dst_ff, _ := NewFlexFile(remote_file_path)

				tb.Run(name, func(t *testing.T) {
					//copy the file
					nfscopy, _ := NewNFSCopy(src_ff, dst_ff, thread_list[thread], 1, 0, 0, true, copyv2_list[copyv2], false)

					_, copy_hash := nfscopy.SpreadCopy()

					if !copyv2_list[copyv2] {
						copy_hash_string := fmt.Sprintf("%x", copy_hash)
						if copy_hash_string != validation_hash {
							t.Logf("Copied hash %s doesn't equal source file hash %s",
								copy_hash_string, local_hashes[local_file_path])
						}
					}

					dst_ff_for_hash_check, _ := NewFlexFile(remote_file_path)
					nfshasher_dst, _ := NewSpreadHash(dst_ff_for_hash_check, thread_list[thread], 1, 0, 0, false)
					_, nfshash_dst := nfshasher_dst.SpreadHash()

					remote_hash_string := fmt.Sprintf("%x", nfshash_dst)
					if remote_hash_string != validation_hash {
						t.Errorf("Destination spread hash %s doesn't equal source file hash %s",
							remote_hash_string, local_hashes[local_file_path])
					}

				})

			}
		}
	}
}
