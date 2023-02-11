package main

import (
	"fmt"
	"os"
	"time"

	//"os/exec"
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
	filesizeG := ff_src.Size / (1024 ^ 3)

	threads := []int{8, 12, 16}
	sizeMB := []int{4, 8, 16, 32}

	for _, thread := range threads {
		for _, size := range sizeMB {

			//run command on shell

			testname := fmt.Sprintf("%dG, %d threads, %d sizeMB", filesizeG, thread, size)

			c := Fbcp_config{forceInputStream: false,
				forceOutputStream: true,
				threads:           thread,
				sizeMB:            size}

			for n := 0; n < b.N; n++ {
				b.Run(testname, func(b *testing.B) {
					//exec.Command("bash", "-c", "echo", "3", ">", "/proc/sys/vm/drop_caches").Run()
					data := []byte("3")
					os.WriteFile("/proc/sys/vm/drop_caches", data, 0644)

					//sleep for 2 second
					time.Sleep(2 * time.Second)
					b.ResetTimer()

					fbcp_stream_copy(c, ff_src, ff_dst)

				})
			}

		}
	}
}
