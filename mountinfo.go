package main

import (
	//"fmt"
	"bytes"
	"os/exec"
	"sort"
	"strconv"

	//"os/exec"
	"bufio"
	"os"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)


type MountEntry struct {
	device string
	mount_point string
	mount_point_depth int
	protocol string
	options string

	//add for ease of use
	nfs bool          //golang default of bool is false.
	nconnect bool     //golang default of bool is false.
	nconnect_value int
}

var mount_entries_global []MountEntry = nil

func getNFSPathFromLocal(local_path string) ( string ) {

	getMounts()
	
	//sort the array based on path depth
	sort.Slice(mount_entries_global[:], func(i, j int) bool {
		return mount_entries_global[i].mount_point_depth > mount_entries_global[j].mount_point_depth
	  })
	
	for _, v := range mount_entries_global{
		if v.nfs {
			if strings.HasPrefix(local_path, v.mount_point) {
				return strings.Replace(local_path, v.mount_point, v.device, 1)
			}
		}
	}
	return ""
}


func getMounts() ([]MountEntry, error) {

	if mount_entries_global != nil {
		return mount_entries_global, nil
	}
	mount_entries_global = make([]MountEntry, 0 )
	
	//example_mount_linux := "192.168.20.171:/System/Volumes/Data/Users/matthewrobertson/nfs\040space\040test /home/sile/test\040mount nfs rw,relatime,vers=3,rsize=1048576,wsize=10"

	var scanner *bufio.Scanner

	if runtime.GOOS == "linux" {
		log.Debug("OS Type is Linux")
		file, err := os.Open("/proc/mounts")
		if err != nil {
			return nil, err
		}
		defer file.Close()
		scanner = bufio.NewScanner(file)
		
	} else if runtime.GOOS == "darwin" {
		log.Debug("OS type is darwin (MacOS)")
		resp, err := exec.Command("mount").Output()
		if err != nil{
			return nil, err
		}
		scanner = bufio.NewScanner(bytes.NewReader(resp))
	} else {
		log.Warnf("Unknown OS: %s, can't process mounts.", runtime.GOOS)
	}

	// read mount line by line
    for scanner.Scan() {
        // do something with a line
		// this will unquote spaces and apostrophes /040 
        line := scanner.Text()
		log.Debugf("%s", line)

		var mount_entry MountEntry
		skip := false

		if runtime.GOOS == "linux" {
			items := strings.Split(line, " ")
			if len(items) < 4 || len(items) > 6 {
				log.Warnf("Unexpected items in mount line, expected 4-6, found %d, \n line: %s", len(items), line)
				skip = true

			} else {
				//	log.Debugf("%s %s %s %s", items[0], items[1], items[2], items[3])	
				var err error
				mount_entry.device, err = strconv.Unquote(items[0])
				if err != nil {
					log.Debugf("conveting device %s failed, results %s", items[0],mount_entry.device)
					log.Debug(err)
				}
				mount_entry.mount_point, err = strconv.Unquote(items[1])
				if err != nil {
					log.Debugf("conveting mount point %s failed, results %s", items[1],mount_entry.mount_point)
					log.Debug(err)
				}
				mount_entry.protocol = items[2]
				mount_entry.options = items[3]
			}
		} else if runtime.GOOS == "darwin" {
			//a contrived path with spaces and "  on "could mess this up.
			items1 := strings.Split(line," on /")
			if len(items1) > 2 {
				log.Warn("Error, unable to parse mount line, found moulitple ' on /' entries ")
				skip = true
			}
			// we added the / to try to ensure to not hit a false positive, lets add it back in
			// this will make the rest of the parsing easier to follow.
			items1[1] = "/" + items1[1]

			mount_entry.device = items1[0]

			// find beginning of options by finding the last "("
			options_begin_index := strings.LastIndex(items1[1], "(")
			mount_entry.mount_point = items1[1][0:options_begin_index - 1]
			//grab the options inside the ( )
			mount_entry.options = items1[1][options_begin_index+1:len(items1[1])-1]
			mount_entry.options = strings.ReplaceAll(mount_entry.options, " ", "")
			//on mac the first option is the device type
			mount_entry.protocol = strings.Split(mount_entry.options, ",")[0]
		}

		if !skip{
			// add additional information

			//mount depth, usefull for matching a local_path
			c := strings.Count(mount_entry.mount_point, string(os.PathSeparator))
			mount_entry.mount_point_depth = c


			if strings.ToLower(mount_entry.protocol) == "nfs"{
				mount_entry.nfs = true

				for _, v := range strings.Split(mount_entry.options, ",") {
					//Example: rw,relatime,vers=3,nconnect=12
					if strings.Contains(v, "nconnect"){
						mount_entry.nconnect = true
						nconnect := strings.Split(v,"=")
						if len(nconnect) == 2 {
							mount_entry.nconnect_value, _ = strconv.Atoi(nconnect[1])
						}
					}
				}
			}

			log.Debugf("Device: %s Mount Point: %s , Proto: %s, nfs %b, nconnect: %b",
				mount_entry.device, mount_entry.mount_point, mount_entry.protocol, 
				mount_entry.nfs, mount_entry.nconnect)

			mount_entries_global = append(mount_entries_global, mount_entry)
		}
    }

	return mount_entries_global, nil
}