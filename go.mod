module github.com/sile16/fbcp

go 1.19

require (
	github.com/sile16/go-nfs-client v0.0.0-20221111005448-63fb5f908fe4
	github.com/sirupsen/logrus v1.9.0
	github.com/vbauerster/mpb/v7 v7.5.3
	github.com/zeebo/xxh3 v1.0.2
)

require (
	github.com/bastjan/netstat v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/prometheus/client_golang v1.14.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

require (
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/klauspost/cpuid/v2 v2.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	github.com/rivo/uniseg v0.4.2 // indirect
	github.com/shirou/gopsutil/v3 v3.22.10
	golang.org/x/sys v0.2.0 // indirect
)

replace github.com/sile16/go-nfs-client => ../go-nfs-client

replace github.com/sile16/go-nfs-client/nfs/metrics => ../go-nfs-client/nfs/metrics
