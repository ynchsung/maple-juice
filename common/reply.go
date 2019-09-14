package common

type ReplyGrep struct {
	Host   string
	Flag   bool
	ErrStr string
	Lines  []Line
}

type ReplyGrepList []*ReplyGrep
