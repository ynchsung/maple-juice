package common

type ArgGrep struct {
	Paths      []string
	RequestStr string
	IsRegex    bool
}

type ArgWriteFile struct {
	Path    string
	Content []byte
}

type ArgMemberJoin HostInfo

type ArgMemberAdd HostInfo
