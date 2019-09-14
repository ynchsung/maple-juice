package common

import (
	"bufio"
	"os"
	"regexp"
)

/* GrepFile
 * use regular expression to grep lines from a file
 * Args:	path, regex
 * Return:	slice of Line, error
 */

type Line struct {
	LineNum int
	LineStr string
}

func GrepFile(path string, regex string) ([]Line, error) {
	re := regexp.MustCompile(regex)

	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	l, lines := 0, []Line{}
	for scanner.Scan() {
		l += 1
		text := scanner.Text()
		if re.MatchString(text) {
			lines = append(lines, Line{l, text})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}
