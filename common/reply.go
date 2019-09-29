package common

type ReplyGrep struct {
	Host      string
	Flag      bool
	ErrStr    string
	LineCount int
	Files     []*GrepInfo
}

type ReplyGrepList []*ReplyGrep

type ReplyWriteFile struct {
	Flag        bool
	ErrStr      string
	ByteWritten int
}

type ReplyMemberJoin struct {
	Flag   bool
	ErrStr string
}

type ReplyMemberAdd struct {
	Flag   bool
	ErrStr string
}
