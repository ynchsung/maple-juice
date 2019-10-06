package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"ycsw/common"
)

const (
	HEARTBEAT_INTERVAL         time.Duration = 3500 * time.Millisecond
	HEARTBEAT_TIMEOUT          time.Duration = 4000 * time.Millisecond
	HEARTBEAT_MONITOR_INTERVAL time.Duration = 500 * time.Millisecond
	HEARTBEAT_WRITE_TIMEOUT    time.Duration = 500 * time.Millisecond
	HEARTBEAT_RECV_BACK        int           = 1
	HEARTBEAT_RECV_AHEAD       int           = 2
)

type HeartbeatMessage struct {
	Info  common.HostInfo `json:"info"`
	Incar int             `json:"incarnation"`
}

func SendHeartbeat(receiver common.MemberInfo, sendByte []byte, c chan error) {
	conn, err := net.Dial("udp", receiver.Info.Host+receiver.Info.UdpPort)
	if err != nil {
		log.Printf("[Error] Fail to send heartbeat to %v: %v", receiver.Info.Host, err)
		c <- err
		return
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(HEARTBEAT_WRITE_TIMEOUT))
	n, err := conn.Write(sendByte)
	if err != nil {
		c <- nil
		return
	} else if n != len(sendByte) {
		c <- errors.New("only partial bytes were sent")
		return
	}

	c <- nil
}

func HeartbeatSender() {
	// initialize random seed and random generator
	seed := rand.NewSource(time.Now().UnixNano())
	rg := rand.New(seed)

	incar := 1

	for {
		now := time.Now()
		receivers := common.GetHeartbeatReceivers(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)
		sendByte, _ := json.Marshal(&HeartbeatMessage{common.Cfg.Self, incar})

		var (
			chans map[string]chan error = make(map[string]chan error)
		)

		for _, receiver := range receivers {
			num := rg.Intn(100)
			if float64(num) >= 100.0*common.Cfg.UdpDropRate-1e-5 {
				chans[receiver.Info.Host] = make(chan error)
				go SendHeartbeat(receiver, sendByte, chans[receiver.Info.Host])
			} else {
				log.Printf("[Test] Udp heartbeat drop, host %v, random number %v (out of 100)",
					receiver.Info.Host,
					num,
				)
			}
		}

		// Wait for all heartbeat sending thread
		for key, _ := range chans {
			err := <-chans[key]
			if err != nil {
				log.Printf("[Error] Fail to send udp heartbeat to %v: %v",
					key,
					err,
				)
			} else {
				log.Printf("[Info] Send udp heartbeat to %v, incarnation %v, timestamp %v",
					key,
					incar,
					now.Unix(),
				)
			}
		}

		incar += 1
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func HandleFailure(failure common.MemberInfo, memberListCopy []common.MemberInfo, c chan error) {
	var tasks []*common.RpcAsyncCallerTask

	args := common.ArgMemberFailure{
		common.Cfg.Self,
		failure.Info,
	}
	for _, mem := range memberListCopy {
		/*
			// don't send failure message to the failing machine
			if mem.Info.Host == failure.Info.Host {
				continue
			}
		*/

		task := &common.RpcAsyncCallerTask{
			"MemberFailure",
			mem.Info,
			&args,
			&common.ReplyMemberFailure{true, ""},
			make(chan error),
		}

		go common.CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil && task.Info.Host != failure.Info.Host {
			log.Printf("[Error] Fail to send MemberFailure to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	c <- nil
}

func HeartbeatMonitor() {
	for {
		now := time.Now()
		senderMap, memberListCopy := common.PrepareHeartbeatInfoForMonitor(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)

		var chans []chan error
		for _, sender := range senderMap {
			if now.After(sender.Timestamp.Add(HEARTBEAT_TIMEOUT)) {
				// handle timeout
				log.Printf("[Info] Detect failure host %v, id %v, incarnation %v, timestamp %v, now %v",
					sender.Info.Host,
					sender.Info.MachineID,
					sender.Incar,
					sender.Timestamp.Unix(),
					now.Unix(),
				)

				fmt.Printf("Detect failure host %v, id %v, incarnation %v, timestamp %v, now %v\n",
					sender.Info.Host,
					sender.Info.MachineID,
					sender.Incar,
					sender.Timestamp.Unix(),
					now.Unix(),
				)

				chans = append(chans, make(chan error))
				go HandleFailure(sender, memberListCopy, chans[len(chans)-1])
			}
		}

		for _, c := range chans {
			_ = <-c
		}

		time.Sleep(HEARTBEAT_MONITOR_INTERVAL)
	}
}
