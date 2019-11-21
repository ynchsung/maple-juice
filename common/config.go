package common

import (
	"encoding/json"
	"errors"
	"gopkg.in/ini.v1"
	"os"
)

type HostInfo struct {
	Host      string `json:"host"`
	Port      string `json:"port"`
	UdpPort   string `json:"udp_port"`
	MachineID int    `json:"machine_id"`
}

type Cluster struct {
	ClusterName string     `json:"cluster_name"`
	Hosts       []HostInfo `json:"hosts"`
}

type Config struct {
	Self             HostInfo
	UdpDropRate      float64
	Introducer       HostInfo
	LogPath          string
	SDFSDir          string
	MapReduceWorkDir string
	ClusterInfo      Cluster
}

var Cfg Config

func ReadMachineBasedConfig(path string) error {
	tmp, err := ini.Load(path)
	if err != nil {
		return err
	}

	Cfg.Self.Host = tmp.Section("machine").Key("Host").String()
	Cfg.Self.MachineID = tmp.Section("machine").Key("ID").MustInt(-1)

	if Cfg.Self.MachineID < 0 {
		return errors.New("machine ID error")
	}

	return nil
}

func ReadClusterConfig(path string) error {
	fp, err := os.Open(path)
	if err != nil {
		return err
	}

	defer fp.Close()

	decoder := json.NewDecoder(fp)
	if err = decoder.Decode(&Cfg.ClusterInfo); err != nil {
		return err
	}

	return nil
}

func ReadConfig(path string) error {
	tmp, err := ini.Load(path)
	if err != nil {
		return err
	}

	Cfg.Self.Port = tmp.Section("server").Key("port").String()
	Cfg.Self.UdpPort = tmp.Section("server").Key("udp_port").String()
	Cfg.UdpDropRate = tmp.Section("server").Key("udp_drop_rate").MustFloat64(0.0)
	Cfg.Introducer.Host = tmp.Section("server").Key("introducer_host").String()
	Cfg.Introducer.Port = tmp.Section("server").Key("introducer_port").String()
	Cfg.Introducer.MachineID = tmp.Section("server").Key("introducer_id").MustInt(-1)
	Cfg.LogPath = tmp.Section("paths").Key("log").String()
	Cfg.SDFSDir = tmp.Section("paths").Key("sdfs_dir").String()
	Cfg.MapReduceWorkDir = tmp.Section("paths").Key("work_dir").String()

	// read cluster config
	if err = ReadClusterConfig(tmp.Section("paths").Key("cluster_info").String()); err != nil {
		return err
	}

	return ReadMachineBasedConfig(tmp.Section("paths").Key("machine_info_config").String())
}
