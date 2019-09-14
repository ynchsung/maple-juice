package common

import (
	"gopkg.in/ini.v1"
)

type Config struct {
	Port    string
	LogPath string
}

var Cfg Config

func ReadConfig(path string) error {
	tmp, err := ini.Load(path)
	if err != nil {
		return err
	}

	Cfg.Port = tmp.Section("server").Key("port").String()
	Cfg.LogPath = tmp.Section("paths").Key("log").String()
	return nil
}
