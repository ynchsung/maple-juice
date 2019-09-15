package common

import (
	"bufio"
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

func GrepFile(path string, regex string) (*GrepInfo, error) {
	re := regexp.MustCompile(regex)

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

func WriteFile(path string, content []byte) (int, error) {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	defer fp.Close()

	return fp.Write(content)
}
