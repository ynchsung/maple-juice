package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"ycsw/common"
)

func InitServer(path string) {
	// read general configure file
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

func UdpServer(udp net.PacketConn) {
	buf := make([]byte, 1024)
	for {
		n, _, err := udp.ReadFrom(buf)
		if err != nil {
			log.Printf("[Error] Fail to read udp packet: %v", err)
			continue
		}

		now := time.Now()
		res := common.MemberInfo{}
		json.Unmarshal(buf[:n], &res)

		if common.UpdateHeartbeat(res, now) {
			log.Printf("[Info] Get heartbeat from %v, id %v, incarnation %v, time %v",
				res.Info.Host,
				res.Info.MachineID,
				res.Incar,
				res.Timestamp,
			)
		}
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

	l, e := net.Listen("tcp", common.Cfg.Self.Port)
	if e != nil {
		log.Fatalf("[Fatal] TCP listen error:", e)
		os.Exit(1)
	}

	log.Printf("[Info] Server start, host %v, port %v, machine ID %v",
		common.Cfg.Self.Host,
		common.Cfg.Self.Port,
		common.Cfg.Self.MachineID,
	)

	// listen rpc
	go http.Serve(l, nil)

	// listen udp
	udp, err := net.ListenPacket("udp", common.Cfg.Self.UdpPort)
	if err != nil {
		log.Fatalf("[Fatal] UDP listen error: %v", err)
		os.Exit(1)
	}
	go UdpServer(udp)

	common.AddMember(common.Cfg.Self)
	if common.Cfg.Self.Host != common.Cfg.Introducer.Host {
		// not introducer
		var (
			args  common.ArgMemberJoin = common.ArgMemberJoin(common.Cfg.Self)
			reply common.ReplyMemberJoin
			c     chan error = make(chan error)
		)
		go common.CallRpcS2SGeneral("MemberJoin", common.Cfg.Introducer.Host, common.Cfg.Introducer.Port, &args, &reply, c)
		err := <-c
		if err != nil {
			log.Fatalf("[Fatal] Join error: %v", err)
			os.Exit(1)
		}
	}

	go HeartbeatSender()
	go HeartbeatMonitor()

	select {}
}
