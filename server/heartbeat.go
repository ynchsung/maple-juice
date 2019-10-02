package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"ycsw/common"
)

const (
	HEARTBEAT_INTERVAL         time.Duration = 3500 * time.Millisecond
	HEARTBEAT_TIMEOUT          time.Duration = 4000 * time.Millisecond
	HEARTBEAT_MONITOR_INTERVAL time.Duration = 500 * time.Millisecond
	HEARTBEAT_RECV_BACK        int           = 1
	HEARTBEAT_RECV_AHEAD       int           = 2
)

func HeartbeatSender() {
	incar := 1

	for {
		now := time.Now()
		receivers := common.GetHeartbeatReceivers(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)
		send_byte, _ := json.Marshal(&common.MemberInfo{common.Cfg.Self, incar, now})

		// TODO: send heartbeat in multi-threads
		for _, receiver := range receivers {
			conn, err := net.Dial("udp", receiver.Info.Host+receiver.Info.UdpPort)
			if err != nil {
				log.Printf("[Error] Fail to send heartbeat to %v: %v", receiver.Info.Host, err)
				continue
			}

			conn.Write(send_byte)
			conn.Close()
			log.Printf("[Info] Send heartbeat to %v, incarnation %v, timestamp %v",
				receiver.Info.Host,
				incar,
				now.Unix(),
			)
		}

		incar += 1
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func HandleFailure(c chan error) {
	var (
		args   common.ArgMemberFailure = common.ArgMemberFailure(sender.Info)
		replys []*common.ReplyMemberFailure
		chans  []chan error
	)

	for _, mem := range memberListCopy {
		// don't send failure message to the failing machine
		if mem.Info.Host == sender.Info.Host {
			continue
		}

		r := &common.ReplyMemberFailure{true, ""}
		c2 := make(chan error)
		go common.CallRpcS2SGeneral("MemberFailure", mem.Info.Host, mem.Info.Port, &args, r, c2)

		replys = append(replys, r)
		chans = append(chans, c2)
	}

	// Wait for all RpcS2S
	for _, c2 := range chans {
		_ = <-c2
	}

	c <- nil
}

func HeartbeatMonitor() {
	for {
		now := time.Now()
		senderMap, memberListCopy := common.PrepareHeartbeatInfoForMonitor(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)

		var (
			chans []chan error
		)

		for _, sender := range senderMap {
			if now.After(sender.Timestamp.Add(HEARTBEAT_TIMEOUT)) {
				// handle timeout
				log.Printf("[Info] Detect failure host %v, id %v, incarnation %v, timestamp %v, now %v",
					sender.Info.Host,
					sender.Info.MachineID,
					sender.Incar,
					sender.Timestamp,
					now.Unix(),
				)

				fmt.Printf("Detect failure host %v, id %v, incarnation %v, timestamp %v, now %v\n",
					sender.Info.Host,
					sender.Info.MachineID,
					sender.Incar,
					sender.Timestamp,
					now.Unix(),
				)

				c := make(chan error)
				go HandleFailure(c)

				chans = append(chans, c)
			}
		}

		for _, c := range chans {
			<-c
		}

		time.Sleep(HEARTBEAT_MONITOR_INTERVAL)
	}
}
