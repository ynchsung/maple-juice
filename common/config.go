package common

import (
	"gopkg.in/ini.v1"
)

type Config struct {
	Port    string
	LogPath string
	Hosts   []string
}

var Cfg Config

func ReadConfig(path string) error {
	tmp, err := ini.Load(path)
	if err != nil {
		return err
	}

	Cfg.Port = tmp.Section("server").Key("port").String()
	Cfg.LogPath = tmp.Section("paths").Key("log").String()
	Cfg.Hosts = append(Cfg.Hosts, "172.22.154.175", "172.22.156.175")
	return nil
}
