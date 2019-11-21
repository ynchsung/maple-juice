package common

import (
	"bufio"
	"io/ioutil"
	"math/rand"
	"os"
	"regexp"
)

/* GrepFile
 * use regular expression to grep lines from a file
 * Args:	path, regex
 * Return:	*GrepInfo, error
 */

type Line struct {
	LineNum int
	LineStr string
}

type GrepInfo struct {
	Path  string
	Lines []Line
}

func GrepFile(path string, str string, isRegex bool) (*GrepInfo, error) {
	if !isRegex {
		str = regexp.QuoteMeta(str)
	}
	re := regexp.MustCompile(str)

	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	l, ret := 0, &GrepInfo{path, []Line{}}
	for scanner.Scan() {
		l += 1
		text := scanner.Text()
		if re.MatchString(text) {
			ret.Lines = append(ret.Lines, Line{l, text})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ret, nil
}

func CreateFile(path string, size int) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer fp.Close()

	for i := 0; i < size; i++ {
		fp.Write([]byte("0"))
	}

	return nil
}

func WriteFile(path string, content []byte) (int, error) {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return 0, err
	}
	defer fp.Close()

	n, err := fp.Write(content)
	content = nil
	return n, err
}

func WriteFileOffset(path string, offset int64, content []byte) (int, error) {
	fp, err := os.OpenFile(path, os.O_WRONLY, 0755)
	if err != nil {
		return 0, err
	}
	defer fp.Close()

	n, err := fp.WriteAt(content, offset)
	content = nil
	return n, err
}

func DeleteFile(path string) error {
	return os.Remove(path)
}

func ReadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func GenRandomString(size int) string {
	const (
		l string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	)

	ret := make([]byte, size)
	for i := 0; i < size; i++ {
		ret[i] = l[rand.Intn(len(l))]
	}

	return string(ret)
}
