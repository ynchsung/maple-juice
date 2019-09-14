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

	// register rpc client
	rpcClient := new(common.RpcClient)
	rpc.Register(rpcClient)

	// register rpc server to server
	rpcS2S := new(common.RpcS2S)
	rpc.Register(rpcS2S)

	rpc.HandleHTTP()

	l, e := net.Listen("tcp", common.Cfg.Port)
	if e != nil {
		log.Fatal("Listen error:", e)
	}
	log.Printf("Server start, port %v", common.Cfg.Port)
	http.Serve(l, nil)
}
