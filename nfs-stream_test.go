package main

import (
	"fmt"
	"os"
	"testing"

	//log "github.com/sirupsen/logrus"
)



func BenchmarkStreamRead(b *testing.B) {

	//log.SetLevel(log.DebugLevel)

	var test_file_name = os.Getenv("TEST_FILE")
	
	if test_file_name == "" {
		test_file_name = "../junk10"
	}

	ff_src, err := NewFlexFile(test_file_name)
	if err != nil {
		b.Error("Could not open test file")
	}

	ff_dst, err2 := NewFlexFile("/dev/null")
	if err2 != nil {
		b.Error("Could not open dev null")
	}
	ff_dst.is_pipe = true
	filesizeG := ff_src.size / (1024 ^ 3)

	
	threads := []int{1, 4, 8, 16}
	sizeMB := []int64{4, 16, 32, 64, 128, 256, 512, 1024}
	
	for _, thread := range threads {
		for _, size := range sizeMB {

			
			testname := fmt.Sprintf("%dG, %d threads, %d sizeMB, DirectNFS %t", filesizeG, thread, size, ff_src.is_nfs)
	
			c := fbcp_config{	forceInputStream : false,
								forceOutputStream : true,
								threads : thread,
								sizeMB : size, }

			for n := 0; n < b.N; n++ {
				b.Run(testname, func (b *testing.B) {
					fbcp_stream_copy(c, ff_src, ff_dst) 

				})
			}
			
		}
	}
}