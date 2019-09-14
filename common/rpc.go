package common

import (
	"net/rpc"
)

type RpcClient struct {
}

type RpcS2S struct {
}

func (t *RpcClient) GrepLogFile(args *ArgGrep, reply *ReplyGrepList) error {
	var (
		clients  []*rpc.Client
		divCalls []*rpc.Call
	)

	for _, host := range Cfg.ClusterInfo.Hosts {
		client, err := rpc.DialHTTP("tcp", host.Host+host.Port)
		var divCall *rpc.Call = nil
		r := &ReplyGrep{host.Host, true, "", 0, []*GrepInfo{}}
		if err == nil {
			divCall = client.Go("RpcS2S.GrepLogFile", args, r, nil)
		} else {
			client = nil
			r.Flag = false
			r.ErrStr = err.Error()
		}

		clients = append(clients, client)
		divCalls = append(divCalls, divCall)
		*reply = append(*reply, r)
	}

	for i, _ := range Cfg.ClusterInfo.Hosts {
		if clients[i] != nil {
			replyCall := <-divCalls[i].Done
			if replyCall.Error != nil {
				(*reply)[i].Flag = false
				(*reply)[i].ErrStr = replyCall.Error.Error()
			}
		}
	}

	return nil
}

func (t *RpcS2S) GrepLogFile(args *ArgGrep, reply *ReplyGrep) error {
	reply.Host = Cfg.Self.Host
	reply.Flag = true
	reply.ErrStr = ""
	reply.LineCount = 0
	reply.Files = []*GrepInfo{}
	for _, path := range args.Paths {
		info, err := GrepFile(path, args.Regex)
		if err == nil {
			reply.LineCount += len(info.Lines)
			reply.Files = append(reply.Files, info)
		}
	}

	return nil
}
