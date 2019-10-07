package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"ycsw/common"
)

const (
	HEARTBEAT_INTERVAL         time.Duration = 2000 * time.Millisecond
	HEARTBEAT_TIMEOUT          time.Duration = 4500 * time.Millisecond
	HEARTBEAT_MONITOR_INTERVAL time.Duration = 500 * time.Millisecond
	HEARTBEAT_WRITE_TIMEOUT    time.Duration = 500 * time.Millisecond
	HEARTBEAT_RECV_BACK        int           = 1
	HEARTBEAT_RECV_AHEAD       int           = 2
)

type HeartbeatMessage struct {
	Info  common.HostInfo `json:"info"`
	Incar int             `json:"incarnation"`
}

type HeartbeatTask struct {
	Target  common.HostInfo
	Message *HeartbeatMessage
	Chan    chan error
}

func SendHeartbeat(task *HeartbeatTask) {
	conn, err := net.Dial("udp", task.Target.Host+task.Target.UdpPort)
	if err != nil {
		task.Chan <- err
		return
	}
	defer conn.Close()

	sendByte, _ := json.Marshal(task.Message)

	// FIXME: not working?
	conn.SetWriteDeadline(time.Now().Add(HEARTBEAT_WRITE_TIMEOUT))

	n, err := conn.Write(sendByte)
	now := time.Now()
	if err != nil {
		task.Chan <- err
		return
	} else if n != len(sendByte) {
		task.Chan <- errors.New("only partial bytes were sent")
		return
	}

	log.Printf("[Info] Send udp heartbeat to %v, incarnation %v, timestamp %v",
		task.Target.Host,
		task.Message.Incar,
		now.Unix(),
	)

	task.Chan <- nil
}

func HeartbeatSender() {
	// waiting queue job
	var (
		waitQueue    []*HeartbeatTask
		waitQueueMux sync.Mutex
	)

	go func() {
		for {
			var hbTask *HeartbeatTask = nil

			waitQueueMux.Lock()
			if len(waitQueue) > 0 {
				hbTask = waitQueue[0]
				waitQueue = waitQueue[1:]
			}
			waitQueueMux.Unlock()

			if hbTask != nil {
				select {
				case err := <-hbTask.Chan:
					if err != nil {
						log.Printf("[Error] Fail to send udp heartbeat to %v (incarnation %v): %v",
							hbTask.Target.Host,
							hbTask.Message.Incar,
							err,
						)
					}
				case <-time.After(100 * time.Millisecond):
					waitQueueMux.Lock()
					waitQueue = append(waitQueue, hbTask)
					waitQueueMux.Unlock()
					log.Printf("[Verbose] Udp heartbeat sending task stuck (host %v, incarnation %v)",
						hbTask.Target.Host,
						hbTask.Message.Incar,
					)
				}
			}

			time.Sleep(time.Second)
		}
	}()

	// initialize random seed and random generator
	seed := rand.NewSource(time.Now().UnixNano())
	rg := rand.New(seed)

	incar := 1

	for {
		receivers := common.GetHeartbeatReceivers(HEARTBEAT_RECV_BACK, HEARTBEAT_RECV_AHEAD)
		hbMessage := HeartbeatMessage{common.Cfg.Self, incar}

		if len(receivers) == 0 {
			continue
		}

		log.Printf("[Verbose] Prepare to send heartbeat to %v hosts, incarnation %v, timestamp %v",
			len(receivers),
			incar,
			time.Now().Unix(),
		)

		for _, receiver := range receivers {
			num := rg.Intn(100)
			if float64(num) >= 100.0*common.Cfg.UdpDropRate-1e-5 {
				task := &HeartbeatTask{
					receiver.Info,
					&hbMessage,
					make(chan error),
				}
				go SendHeartbeat(task)

				waitQueueMux.Lock()
				waitQueue = append(waitQueue, task)
				waitQueueMux.Unlock()
			} else {
				log.Printf("[Test] Udp heartbeat drop, host %v, random number %v (out of 100)",
					receiver.Info.Host,
					num,
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
