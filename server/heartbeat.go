package main

import (
	"encoding/json"
	"log"
	"net"
	"time"

	"ycsw/common"
)

const (
	HEARTBEAT_INTERVAL   = 5
	HEARTBEAT_RECV_BACK  = 1
	HEARTBEAT_RECV_AHEAD = 2
)

func HeartbeatSender() {
	incar := 1

	for {
		receivers := common.GetHeartbeatReceivers(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)
		send_byte, _ := json.Marshal(&common.MemberInfo{common.Cfg.Self, incar, time.Now()})

		for _, receiver := range receivers {
			conn, err := net.Dial("udp", receiver.Info.Host+receiver.Info.UdpPort)
			if err != nil {
				log.Printf("[Error] Fail to send heartbeat to %v: %v", receiver.Info.Host, err)
				continue
			}

			conn.Write(send_byte)
			conn.Close()
		}

		incar += 1
		time.Sleep(HEARTBEAT_INTERVAL * time.Second)
	}
}

func HeartbeatMonitor() {
	for {
		time.Sleep(time.Second)
	}
}
