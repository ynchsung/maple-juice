package common

import (
	"log"
	"time"
)

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepFile(args *ArgGrep, reply *ReplyGrepList) error {
	var tasks []*RpcAsyncCallerTask

	// Send RpcS2S Grep File for all machines in the cluster
	for _, host := range Cfg.ClusterInfo.Hosts {
		task := &RpcAsyncCallerTask{
			"GrepFile",
			host,
			args,
			&ReplyGrep{host.Host, true, "", 0, []*GrepInfo{}},
			make(chan error),
		}

		go CallRpcS2SGrepFile(host.Host, host.Port, args, task.Reply.(*ReplyGrep), task.Chan)

		tasks = append(tasks, task)
		*reply = append(*reply, task.Reply.(*ReplyGrep))
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send GrepFile to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	return nil
}

func (t *RpcClient) GetMachineID(args *ArgGetMachineID, reply *ReplyGetMachineID) error {
	*reply = ReplyGetMachineID(Cfg.Self)
	return nil
}

func (t *RpcClient) GetMemberList(args *ArgGetMemberList, reply *ReplyGetMemberList) error {
	*reply = GetMemberList()
	return nil
}

func (t *RpcClient) MemberJoin(args *ArgClientMemberJoin, reply *ReplyClientMemberJoin) error {
	if InMemberList(Cfg.Self) {
		reply.Flag = false
		reply.ErrStr = "Already join"
		return nil
	}

	AddMember(Cfg.Self)
	if Cfg.Self.Host != Cfg.Introducer.Host {
		// not introducer
		args2 := ArgMemberJoin(Cfg.Self)

		task := RpcAsyncCallerTask{
			"MemberJoin",
			Cfg.Introducer,
			&args2,
			&ReplyMemberJoin{},
			make(chan error),
		}

		go CallRpcS2SGeneral(&task)

		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Join error: %v", err)
			reply.Flag = false
			reply.ErrStr = err.Error()
			return nil
		}
	}

	log.Printf("[Info] Join")
	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcClient) MemberLeave(args *ArgClientMemberLeave, reply *ReplyClientMemberLeave) error {
	members := GetMemberList()
	EraseMemberList()

	time.Sleep(time.Second)

	// Send RpcS2S Leave to all members
	var tasks []*RpcAsyncCallerTask
	args2 := ArgMemberLeave(Cfg.Self)
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}

		task := &RpcAsyncCallerTask{
			"MemberLeave",
			mem.Info,
			&args2,
			&ReplyMemberLeave{true, ""},
			make(chan error),
		}

		go CallRpcS2SGeneral(task)

		tasks = append(tasks, task)
	}

	// Wait for all RpcAsyncCallerTask
	for _, task := range tasks {
		err := <-task.Chan
		if err != nil {
			log.Printf("[Error] Fail to send MemberLeave to %v: %v",
				task.Info.Host,
				err,
			)
		}
	}

	log.Printf("[Info] Leave")
	reply.Flag = true
	reply.ErrStr = ""

	return nil
}

func (t *RpcS2S) GrepFile(args *ArgGrep, reply *ReplyGrep) error {
	reply.Host = Cfg.Self.Host
	reply.Flag = true
	reply.ErrStr = ""
	reply.LineCount = 0
	reply.Files = []*GrepInfo{}
	for _, path := range args.Paths {
		info, err := GrepFile(path, args.RequestStr, args.IsRegex)
		if err == nil {
			reply.LineCount += len(info.Lines)
			reply.Files = append(reply.Files, info)
		}
	}

	return nil
}

func (t *RpcS2S) WriteFile(args *ArgWriteFile, reply *ReplyWriteFile) error {
	var err error = nil

	reply.Flag = true
	reply.ErrStr = ""
	reply.ByteWritten, err = WriteFile(args.Path, args.Content)
	if err != nil {
		reply.Flag = false
		reply.ErrStr = err.Error()
		reply.ByteWritten = 0
	}

	return nil
}

func (t *RpcS2S) MemberJoin(args *ArgMemberJoin, reply *ReplyMemberJoin) error {
	log.Printf("[Info] MemberJoin: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	if Cfg.Self.Host == Cfg.Introducer.Host {
		// introducer should forward add member to all other members
		var tasks []*RpcAsyncCallerTask

		members := GetMemberList()
		for _, mem := range members {
			if mem.Info.Host == Cfg.Self.Host {
				continue
			}

			task := &RpcAsyncCallerTask{
				"MemberJoin",
				mem.Info,
				args,
				&ReplyMemberJoin{true, ""},
				make(chan error),
			}

			go CallRpcS2SGeneral(task)

			tasks = append(tasks, task)
		}

		// Wait for all RpcAsyncCallerTask
		for _, task := range tasks {
			err := <-task.Chan
			if err != nil {
				log.Printf("[Error] Fail to send MemberJoin to %v: %v",
					task.Info.Host,
					err,
				)
			}
		}
	}

	// ping the new node to add myself
	arg2 := ArgMemberAdd(Cfg.Self)

	task := RpcAsyncCallerTask{
		"MemberAdd",
		HostInfo{args.Host, args.Port, "", 0},
		&arg2,
		&ReplyMemberAdd{},
		make(chan error),
	}

	go CallRpcS2SGeneral(&task)

	err := <-task.Chan
	if err != nil {
		log.Printf("[Error] Fail to send MemberAdd to %v: %v",
			args.Host,
			err,
		)
	}

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberAdd(args *ArgMemberAdd, reply *ReplyMemberAdd) error {
	log.Printf("[Info] MemberAdd: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberLeave(args *ArgMemberLeave, reply *ReplyMemberLeave) error {
	log.Printf("[Info] MemberLeave: host %v, port %v, udp_port %v, id %v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	DeleteMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberFailure(args *ArgMemberFailure, reply *ReplyMemberFailure) error {
	// check if monitor is in the MemberList
	if !InMemberList(args.MonitorInfo) {
		return nil
	}

	log.Printf("[Info] MemberFailure (detected by host %v): host %v, port %v, udp_port %v, id %v",
		args.MonitorInfo.Host,
		args.FailureInfo.Host,
		args.FailureInfo.Port,
		args.FailureInfo.UdpPort,
		args.FailureInfo.MachineID,
	)

	DeleteMember(args.FailureInfo)
	return nil
}
