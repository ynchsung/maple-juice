package common

import (
	"bufio"
	"os"
	"regexp"
)

func GrepFile(path string, regex string) ([]int, []string, error) {
	re := regexp.MustCompile(regex)

	fp, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	l, lineIndices, lines := 0, []int{}, []string{}
	for scanner.Scan() {
		l += 1
		text := scanner.Text()
		if re.MatchString(text) {
			lineIndices = append(lineIndices, l)
			lines = append(lines, text)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return lineIndices, lines, nil
}
