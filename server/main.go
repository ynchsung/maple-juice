package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"ycsw/common"
)

func init_server(path string) {
	// read configure file
	if err := ReadConfig(path); err != nil {
		fmt.Printf("Fail to read configure file: %v", err)
		os.Exit(1)
	}

	// init log
	fp, err := os.OpenFile(cfg.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Set log file error: %v", err)
		os.Exit(1)
	}
	log.SetOutput(fp)
}

func main() {
	init_server("server.ini")

	// register rpc
	ycsw := new(common.YCSW)
	rpc.Register(ycsw)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", cfg.Port)
	if e != nil {
		log.Fatal("Listen error:", e)
	}

	log.Printf("Server start, port %v", cfg.Port)
	http.Serve(l, nil)
}
