package common

import (
	"log"
	"os"
)

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepFile(args *ArgGrep, reply *ReplyGrepList) error {
	var chans []chan error

	// Send RpcS2S Grep File for all machines in the cluster
	for _, host := range Cfg.ClusterInfo.Hosts {
		r := &ReplyGrep{host.Host, true, "", 0, []*GrepInfo{}}
		c := make(chan error)
		go CallRpcS2SGrepFile(host.Host, host.Port, args, r, c)

		*reply = append(*reply, r)
		chans = append(chans, c)
	}

	// Wait for all RpcS2S
	for i, _ := range Cfg.ClusterInfo.Hosts {
		_ = <-chans[i]
	}

	return nil
}

func (t *RpcClient) Shutdown(args *ArgShutdown, reply *ReplyShutdown) error {
	var (
		args2  ArgMemberLeave = ArgMemberLeave(Cfg.Self)
		replys []ReplyMemberLeave
		chans  []chan error
	)

	// Send RpcS2S Leave to all members
	members := GetMemberList()
	for _, mem := range members {
		if mem.Info.Host == Cfg.Self.Host {
			continue
		}

		r := ReplyMemberLeave{true, ""}
		c := make(chan error)
		go CallRpcS2SGeneral("MemberLeave", mem.Info.Host, mem.Info.Port, &args2, &r, c)

		replys = append(replys, r)
		chans = append(chans, c)
	}

	// Wait for all RpcS2S
	for i, _ := range chans {
		_ = <-chans[i]
	}

	log.Printf("Shutdown")
	os.Exit(0)

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
	log.Printf("MemberJoin: host=%v port=%v udp_port=%v id=%v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	if Cfg.Self.Host == Cfg.Introducer.Host {
		// introducer should forward add member to all other members
		var (
			chans  []chan error
			replys []ReplyMemberJoin
		)

		members := GetMemberList()
		for _, mem := range members {
			if mem.Info.Host == Cfg.Self.Host {
				continue
			}

			r := ReplyMemberJoin{true, ""}
			c := make(chan error)
			go CallRpcS2SGeneral("MemberJoin", mem.Info.Host, mem.Info.Port, args, &r, c)

			replys = append(replys, r)
			chans = append(chans, c)
		}

		// Wait for all RpcS2S
		for i, _ := range chans {
			_ = <-chans[i]
		}
	}

	// ping the new node to add myself
	var (
		arg2   ArgMemberAdd = ArgMemberAdd(Cfg.Self)
		reply2 ReplyMemberAdd
		c2     chan error = make(chan error)
	)
	go CallRpcS2SGeneral("MemberAdd", args.Host, args.Port, &arg2, &reply2, c2)
	_ = <-c2 // assume no error

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberAdd(args *ArgMemberAdd, reply *ReplyMemberAdd) error {
	log.Printf("MemberAdd: host=%v port=%v udp_port=%v id=%v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	AddMember(HostInfo(*args))
	return nil
}

func (t *RpcS2S) MemberLeave(args *ArgMemberLeave, reply *ReplyMemberLeave) error {
	log.Printf("MemberLeave: host=%v port=%v udp_port=%v id=%v",
		args.Host,
		args.Port,
		args.UdpPort,
		args.MachineID,
	)

	DeleteMember(HostInfo(*args))
	return nil
}
