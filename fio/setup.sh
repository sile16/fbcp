#!/bin/bash
# for RHEL 8.7

#update and upgrade
yum update -y
yum upgrade -y

#install openssh-server, git, and nfs-utils
yum install -y openssh-server git nfs-utils fio

# golang install
# wget https://go.dev/dl/go1.20.2.linux-amd64.tar.gz
# rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.2.linux-amd64.tar.gz
# export PATH=$PATH:/usr/local/go/bin
# echo "export PATH=$PATH:/usr/local/go/bin" >> /etc/profile
# go version

git clone https://github.com/sile16/fbcp.git
cd fbcp
mkdir /mnt/fb200
mount -t nfs -o vers=3 192.168.20.20:/data /mnt/fb200
./fbcp -checkmount /mnt/fb200

# run manual fio to test network
# fio --numjobs=16 --size 100M --nrfiles 4 --group_reporting libnfs_write.fio
# fio --numjobs=16 --size 100M --nrfiles 4 --group_reporting libnfs_read.fio


