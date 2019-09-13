package main

import (
	"gopkg.in/ini.v1"
)

type Config struct {
	Port    string
	LogPath string
}

var cfg Config

func ReadConfig(path string) error {
	tmp, err := ini.Load(path)
	if err != nil {
		return err
	}

	cfg.Port = tmp.Section("server").Key("port").String()
	cfg.LogPath = tmp.Section("paths").Key("log").String()
	return nil
}
