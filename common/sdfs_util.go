package common

import (
	"errors"
)

func SDFSDownloadFile(filename string, host HostInfo) ([]byte, int, error) {
	args := &ArgClientGetFile{filename}
	reply := new(ReplyClientGetFile)

	task := &RpcAsyncCallerTask{
		"GetFile",
		host,
		args,
		reply,
		make(chan error),
	}

	go CallRpcClientGeneral(task)

	err := <-task.Chan
	if err == nil && !reply.Flag {
		err = errors.New(reply.ErrStr)
	}

	return reply.Content, reply.Length, err
}
