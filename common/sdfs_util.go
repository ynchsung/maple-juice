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

func SDFSUploadFile(filename string, host HostInfo, content []byte) error {
	args := &ArgClientUpdateFile{
		GenRandomString(16),
		filename,
		false,
		len(content),
		0,
		content,
		true,
	}
	reply := new(ReplyClientUpdateFile)

	task := RpcAsyncCallerTask{
		"UpdateFile",
		host,
		args,
		reply,
		make(chan error),
	}

	go CallRpcClientGeneral(&task)

	err := <-task.Chan
	if err == nil && !reply.Flag {
		err = errors.New(reply.ErrStr)
	}

	return err
}
