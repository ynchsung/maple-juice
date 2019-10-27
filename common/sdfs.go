package common

import (
	"hash/fnv"
	"path/filepath"
	"regexp"
)

func SDFSHash(filename string) int {
	const M uint32 = 11

	h := fnv.New32a()
	h.Write([]byte(filename))
	return int(h.Sum32() % M)
}

func SDFSPath(filename string) string {
	r := regexp.MustCompile("/")
	return filepath.Join(Cfg.SDFSDir, r.ReplaceAllString(filename, "_"))
}

func SDFSPutFile(filename string, length int, content []byte) error {
	_, err := WriteFile(SDFSPath(filename), content[0:length])
	return err
}
