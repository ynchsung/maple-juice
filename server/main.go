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

func InitServer(path string) {
	// read configure file
	if err := common.ReadConfig(path); err != nil {
		fmt.Printf("Fail to read configure file: %v", err)
		os.Exit(1)
	}

	// init log
	if err := common.InitLog(common.Cfg.LogPath); err != nil {
		fmt.Printf("Init log file error: %v", err)
		os.Exit(1)
	}
}

func main() {
	InitServer("server.ini")

	// register rpc
	ycsw := new(common.YCSW)
	rpc.Register(ycsw)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", common.Cfg.Port)
	if e != nil {
		log.Fatal("Listen error:", e)
	}

	log.Printf("Server start, port %v", common.Cfg.Port)
	go http.Serve(l, nil)

	client, err := rpc.DialHTTP("tcp", "172.22.154.175"+common.Cfg.Port)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := &common.Args{"712.*"}
	var reply common.Reply
	err = client.Call("YCSW.GrepLogFile", args, &reply)
	if err == nil {
		for i, idx := range reply.LineIndices {
			fmt.Printf("Get %v: %v\n", idx, reply.Lines[i])
		}
	}
}
