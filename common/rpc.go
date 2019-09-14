package common

import (
	"log"
)

type Args struct {
	Request string
}

type Reply struct {
	LineIndices []int
	Lines       []string
}

type YCSW struct {
}

func (t *YCSW) GrepLogFile(args *Args, reply *Reply) error {
	var err error
	defer func() {
		if err != nil {
			log.Printf("GrepLogFile error: %v", err)
		}
	}()

	reply.LineIndices, reply.Lines, err = GrepFile(Cfg.LogPath, args.Request)
	return err
}
