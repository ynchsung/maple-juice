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
		r := &ReplyGrep{host.Host, true, "", nil}
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
	var err error = nil
	reply.Lines, err = GrepFile(Cfg.LogPath, args.Request)
	if err != nil {
		reply.Host = Cfg.Self.Host
		reply.Flag = false
		reply.ErrStr = err.Error()
	}

	return nil
}
