package main

import (
	"flag"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// ping ip with do not fragment flag set to check jumbo frame support
func check_mtu(ip string) (int, error) {
	// ping with 1472 bytes payload
	// 1472 = 1500 (MTU) - 8 (ICMP header) - 20 (IP header)
	// 8972 = 9000 (MTU) * 8 (ICMP header) - 20 (IP header)
	cmd := exec.Command("ping", "-c", "1", "-s", "8972", "-M", "do", ip)
	err := cmd.Run()
	if err != nil {
		log.Errorf("MTU failed not jumbo frames. ping %s failed: %s", ip, err)
		return 0, err
	}
	return 1500, nil
}

// check rpc slot size
// read the file /proc/sys/sunrpc/tcp_max_slot_table_entries

func check_rpc_slot_size(size int) (int, error) {
	value, err := check_file_value("/proc/sys/sunrpc/tcp_max_slot_table_entries", fmt.Sprintf("%d", size))
	if err != nil {
		return 0, err
	}
	rpc_value, _ := strconv.Atoi(value)

	if rpc_value != size {
		log.Errorf("rpc slot size is not %d, it is %d", size, rpc_value)
	}
	return rpc_value, nil

}

func sysctl_check_value(name string, value string) (bool, error) {
	cmd := exec.Command("sysctl", "-n", name)
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	set_value := strings.TrimSuffix(string(out[:]), "\n")
	if set_value == value {
		return true, nil
	} else {
		log.Errorf("sysctl %s is %s, expected %s", name, set_value, value)
		return false, nil
	}
}

func sysctl_set_value(name string, value string) (bool, error) {
	cmd := exec.Command("sysctl", "-w", name, value)
	err := cmd.Run()
	if err != nil {
		return false, err
	}
	return true, nil
}

func check_file_value(file string, value string) (string, error) {

	cmd := exec.Command("cat", file)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	out_s := string(out[:])

	if out_s == value {
		return value, nil
	} else {
		err := fmt.Errorf("file %s is %s, expected %s", file, out_s, value)
		log.Errorf("%s", err)
		return out_s, err
	}

}

// write value into file and check if it was written correctly
func set_file_value(file string, value string) (string, error) {
	cmd := exec.Command("echo", value, ">", file)
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return check_file_value(file, value)
}

// todo:
// txquelen via ifconfig to 10000[/quote] ?

func check_tcp(tcp_checks map[string]string) (bool, error) {

	// check if the tcp settings are correct
	all_correct := true
	for name, value := range tcp_checks {
		correct, err := sysctl_check_value(name, value)
		if err != nil {
			return false, err
		}
		if !correct {
			all_correct = false
		}
	}
	return all_correct, nil
}

func set_tcp(tcp_checks map[string]string) (bool, error) {
	check_tcp(tcp_checks)

	// check if the tcp settings are correct
	for name, value := range tcp_checks {
		_, err := sysctl_set_value(name, value)
		if err != nil {
			log.Errorf("error setting %s to %s", name, value)
		}
	}
	check_tcp(tcp_checks)
	return true, nil
}

func check_read_ahead_kb(me *MountEntry, value int) (int, error) {

	file_path := fmt.Sprintf("/sys/class/bdi/%s/read_ahead_kb", me.blk_id)
	actual_value, err := check_file_value(file_path, "16384")
	if err != nil {
		return 0, err
	}
	actual_value_i, _ := strconv.Atoi(actual_value)
	if actual_value_i != value {
		log.Errorf("read_ahead_kb for %s is %d, expected %d", me.blk_id, actual_value_i, value)
	}
	return actual_value_i, nil
}

func set_read_ahead_kb(me *MountEntry, value int) {
	check_read_ahead_kb(me, value)
	file_path := fmt.Sprintf("/sys/class/bdi/%s/read_ahead_kb", me.blk_id)
	set_file_value(file_path, fmt.Sprintf("%d", value))
}

func check_mount(me *MountEntry) (bool, error) {

	if me.rsize != 0 && me.rsize != 512*1024 {
		log.Errorf("rsize is %d, expected 512k", me.rsize)
	}
	if me.wsize != 0 && me.wsize != 512*1024 {
		log.Errorf("wsize is %d, expected 512k", me.wsize)
	}
	return true, nil
}

// check rpc slot size
// read the file /proc/sys/sunrpc/tcp_max_slot_table_entries
// set to 128 : https://access.redhat.com/solutions/6535871
// must re-mount after changing this value
func set_rpc_slot_size(size int) (int, error) {
	set_file_value("/proc/sys/sunrpc/tcp_max_slot_table_entries", fmt.Sprintf("%d", size))
	return check_rpc_slot_size(size)
}

func usage() int {
	fmt.Printf("Usage: fbcp -fbcheck [-fix] [local_fs_mountpoint]")
	return 1
}

func fbcheck(c Fbcp_config) int {
	// use flag to read in a string mountpoint
	if flag.NArg() != 1 {
		return usage()
	}

	tcp_checks := map[string]string{
		"net.core.rmem_max":           "16777216",
		"net.core.wmem_max":           "16777216",
		"net.core.rmem_default":       "262144",
		"net.core.wmem_default":       "262144",
		"net.ipv4.tcp_rmem":           "32768 262144 16777216",
		"net.ipv4.tcp_wmem":           "32768 262144 16777216",
		"net.core.netdev_max_backlog": "100000",
		"net.ipv4.tcp_window_scaling": "1",
		"net.ipv4.tcp_sack":           "1",
		"net.ipv4.tcp_timestamps":     "0",
	}

	mountpoint_s := flag.Arg(0)
	getMounts()
	AddMountBlk_id()
	_, me := GetNFSPathFromLocal(mountpoint_s)

	if c.fix {
		if me != nil {
			check_mount(me)
			check_mtu(me.ip)
			check_read_ahead_kb(me, 16384)
		}
		set_rpc_slot_size(128)
		set_tcp(tcp_checks)

	} else {
		if me != nil {
			check_mount(me)
			check_mtu(me.ip)
			set_read_ahead_kb(me, 16384)
		}
		check_rpc_slot_size(128)
		check_tcp(tcp_checks)
	}
	return 0
}
