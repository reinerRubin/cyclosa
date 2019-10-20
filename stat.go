package cyclosa

import (
	"fmt"
	"regexp"
	"strconv"
)

type (
	query             string
	querySector       uint32
	queryStat         map[query]uint
	queryStatBySector map[querySector]queryStat
)

func (s querySector) String() string {
	return fmt.Sprintf("%d", uint32(s))
}

var queryStatStringRegexp = regexp.MustCompile(`(.*)\t(\d+)`)

func queryStatToString(q query, quantity uint) string {
	return fmt.Sprintf("%s\t%d", q, quantity)
}

// manual parsing would be faster but whatever
func queryStatFromString(s string) (query, uint, error) {
	stats := queryStatStringRegexp.FindStringSubmatch(s)
	if len(stats) != 3 {
		return "", 0, fmt.Errorf("cant parse stat line: %s", s)
	}

	quantity, err := strconv.ParseUint(stats[2], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("cant parse count %s: %s", stats[2], err)
	}

	return query(stats[1]), uint(quantity), nil
}
