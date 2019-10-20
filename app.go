package cyclosa

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

const tempDirPrefix = "cyclosa"

// AppSettings TBD
type AppSettings struct {
	InputFilename   string
	OutputFilename  string
	KeyLimit        uint
	SectorsQuantity uint
	RemoveTempFiles bool
}

// Run TBD
func Run(s *AppSettings) error {
	workdir, err := tempWorkdir()
	if err != nil {
		return fmt.Errorf("cant create temp workdir: %s", err)
	}
	defer removeWorkdir(workdir, s.RemoveTempFiles)

	inputFile, err := os.Open(s.InputFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", s.InputFilename, err)
	}
	defer inputFile.Close()

	fanout := newQueryFanout(s.SectorsQuantity, s.KeyLimit, workdir)
	artifacts, err := fanoutQueriesStat(inputFile, fanout)
	if err != nil {
		return fmt.Errorf("cant fanout stats: %s", err)
	}

	if err := inputFile.Close(); err != nil {
		return fmt.Errorf("cant close input file (%s): %s", s.InputFilename, err)
	}

	if err := mergeStats(artifacts, s.OutputFilename); err != nil {
		return fmt.Errorf("cant merge artifacts from %s to %s: %s",
			workdir, s.OutputFilename, err)
	}

	return nil
}

func fanoutQueriesStat(input io.Reader, fanout *queryStatFanout) ([]string, error) {
	inputScanner := bufio.NewScanner(input)
	for inputScanner.Scan() {
		queryLine := inputScanner.Text()
		err := fanout.pushQuery(query(queryLine))
		if err != nil {
			return nil, fmt.Errorf("cant add query (%s) to fanout: %s", queryLine, err)
		}
	}
	if err := inputScanner.Err(); err != nil {
		return nil, fmt.Errorf("fail at reading: %s", err)
	}

	if err := fanout.flush(); err != nil {
		return nil, fmt.Errorf("cant flush fanout: %s", err)
	}

	return fanout.writtenArtifacts(), nil
}

func removeWorkdir(path string, remove bool) {
	if remove {
		err := os.RemoveAll(path)
		if err != nil {
			log.Printf("cant remove workdir (%s): %s", path, err)
		}
		return
	}

	log.Printf("temp workdir still there: %s", path)
}

func tempWorkdir() (string, error) {
	dir, err := ioutil.TempDir("", tempDirPrefix)
	if err != nil {
		return "", fmt.Errorf("cant create temp dir for %s: %s", tempDirPrefix, err)
	}

	return dir, nil
}
