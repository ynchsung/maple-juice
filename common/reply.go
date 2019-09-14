package common

type ReplyGrep struct {
	Host      string
	Flag      bool
	ErrStr    string
	LineCount int
	Files     []*GrepInfo
}

type ReplyGrepList []*ReplyGrep
