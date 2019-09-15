package common

type ArgGrep struct {
	Paths []string
	Regex string
}

type ArgWriteFile struct {
	Path    string
	Content []byte
}
