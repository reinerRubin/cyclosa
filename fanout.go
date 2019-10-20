package cyclosa

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
)

type queryStatFanout struct {
	queryStatBySector queryStatBySector

	queryStatKeyLimit uint
	sectorsQuantity   uint
	totalKeys         uint

	workdir string

	artifacts map[string]struct{}
}

func newQueryFanout(sectorsQuantity, keyLimit uint, workdir string) *queryStatFanout {
	return &queryStatFanout{
		queryStatKeyLimit: keyLimit,
		sectorsQuantity:   sectorsQuantity,
		workdir:           workdir,

		queryStatBySector: make(queryStatBySector, sectorsQuantity),
		artifacts:         make(map[string]struct{}, 0),
	}
}

func (qf *queryStatFanout) pushQuery(query query) error {
	exist := qf.exist(query)
	if exist {
		qf.add(query)
		return nil
	}

	if qf.full() {
		err := qf.flush()
		if err != nil {
			return fmt.Errorf("cant flush stats: %s", err)
		}
	}

	qf.register(query)

	return nil
}

func (qf *queryStatFanout) register(q query) {
	qf.totalKeys++
	qf.add(q)
}

func (qf *queryStatFanout) add(q query) {
	sector := qf.getQuerySector(q)
	statForQuery, sectorFound := qf.queryStatBySector[sector]
	if !sectorFound {
		statForQuery = make(queryStat, 0)
		qf.queryStatBySector[sector] = statForQuery
	}

	statForQuery[q]++
}

func (qf *queryStatFanout) exist(q query) bool {
	sector := qf.getQuerySector(q)
	statForQuery, sectorFound := qf.queryStatBySector[sector]
	if !sectorFound {
		return false
	}

	_, found := statForQuery[q]
	return found
}

func (qf *queryStatFanout) full() bool {
	return qf.totalKeys > qf.queryStatKeyLimit
}

func (qf *queryStatFanout) flush() error {
	for sector, stat := range qf.queryStatBySector {
		statRecords := uint(len(stat))

		err := qf.flushSector(sector, stat)
		if err != nil {
			return fmt.Errorf("cant flush sector (%s): %s", sector, err)
		}

		qf.totalKeys -= statRecords

		delete(qf.queryStatBySector, sector)
	}

	return nil
}

func (qf *queryStatFanout) flushSector(s querySector, sectorStat queryStat) error {
	existSectorStatFilename := qf.workdirFilename(qf.filenameBySector(s))
	existStatPresent, err := qf.fileExist(existSectorStatFilename)
	if err != nil {
		return fmt.Errorf("cant check if exist stat (%s) present: %s",
			existSectorStatFilename, err)
	}

	if existStatPresent {
		err = qf.mergeStat(existSectorStatFilename, sectorStat)
		if err != nil {
			return fmt.Errorf("cant merge stat: %s", err)
		}

		return nil
	}

	err = qf.writeStat(existSectorStatFilename, sectorStat)
	if err != nil {
		return fmt.Errorf("cant write stat to %s: %s", existSectorStatFilename, err)
	}

	qf.artifacts[existSectorStatFilename] = struct{}{}

	return nil
}

func (qf *queryStatFanout) mergeStat(existStatFilename string, sectorStat queryStat) error {
	existStatFile, err := os.Open(existStatFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", existStatFilename, err)
	}
	defer existStatFile.Close()

	mergeFilename := qf.mergeFilenameByStatFilename(existStatFilename)
	mergeFile, err := os.Create(mergeFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", existStatFilename, err)
	}
	defer mergeFile.Close()

	if err := qf.mergeStatFromFile(existStatFile, sectorStat, mergeFile); err != nil {
		return fmt.Errorf("cant merge stat to %s: %s", mergeFilename, err)
	}
	if err := existStatFile.Close(); err != nil {
		return fmt.Errorf("cant close stat file (%s): %s", existStatFilename, err)
	}

	if err := qf.writeStatToFile(sectorStat, mergeFile); err != nil {
		return fmt.Errorf("cant flush stat to file (%s): %s", mergeFilename, err)
	}

	if err := mergeFile.Close(); err != nil {
		return fmt.Errorf("cant close merge stat file (%s): %s", mergeFilename, err)
	}

	if err := os.Rename(mergeFilename, existStatFilename); err != nil {
		return fmt.Errorf("cant move merge stat to stat (mv %s %s): %s",
			mergeFilename, existStatFilename, err)
	}

	return nil
}

func (qf *queryStatFanout) mergeStatFromFile(
	existedStat io.Reader,
	ongoingStat queryStat,
	out io.StringWriter,
) error {
	existedStatScanner := bufio.NewScanner(existedStat)
	for existedStatScanner.Scan() {
		statLine := existedStatScanner.Text()
		query, quantity, err := queryStatFromString(statLine)
		if err != nil {
			return fmt.Errorf("cant parse stat line (%s): %s", statLine, err)
		}

		ongoingQuantity, found := ongoingStat[query]
		if found {
			delete(ongoingStat, query)
		}
		quantity += ongoingQuantity

		_, err = out.WriteString(qf.statString(query, quantity))
		if err != nil {
			return fmt.Errorf("cant write stat to merge file: %s", err)
		}
	}
	if err := existedStatScanner.Err(); err != nil {
		return fmt.Errorf("fail at reading: %s", err)
	}

	return nil
}

func (qf *queryStatFanout) writeStatToFile(ongoingStat queryStat, out io.StringWriter) error {
	for query, quantity := range ongoingStat {
		_, err := out.WriteString(qf.statString(query, quantity))
		if err != nil {
			return fmt.Errorf("cant write stat to out file: %s", err)
		}
	}

	return nil
}

func (qf *queryStatFanout) writeStat(existStatFilename string, sectorStat queryStat) error {
	existStatFile, err := os.Create(existStatFilename)
	if err != nil {
		return fmt.Errorf("cant open file (%s): %s", existStatFilename, err)
	}
	defer existStatFile.Close()

	if err := qf.writeStatToFile(sectorStat, existStatFile); err != nil {
		return fmt.Errorf("cant flush stat to file (%s): %s", existStatFilename, err)
	}

	if err := existStatFile.Close(); err != nil {
		return fmt.Errorf("cant close stat file (%s): %s", existStatFilename, err)
	}

	return nil
}

func (qf *queryStatFanout) getQuerySector(q query) querySector {
	h := fnv.New32a()
	h.Write([]byte(q))
	return querySector(uint(h.Sum32()) % qf.sectorsQuantity)
}

func (qf *queryStatFanout) workdirFilename(filename string) string {
	return path.Join(qf.workdir, filename)
}

func (qf *queryStatFanout) fileExist(filename string) (bool, error) {
	if _, found := qf.artifacts[filename]; found {
		return true, nil
	}

	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cant check file existing (%s): %s", filename, err)
	}

	return !info.IsDir(), nil
}

func (qf *queryStatFanout) statString(q query, quantity uint) string {
	return queryStatToString(q, quantity) + "\n"
}

func (qf *queryStatFanout) filenameBySector(s querySector) string {
	return fmt.Sprintf("%s", s)
}

func (qf *queryStatFanout) mergeFilenameByStatFilename(p string) string {
	return p + ".merge"
}

func (qf *queryStatFanout) writtenArtifacts() []string {
	artifacts := make([]string, 0, len(qf.artifacts))
	for artifact := range qf.artifacts {
		artifacts = append(artifacts, artifact)
	}
	return artifacts
}
