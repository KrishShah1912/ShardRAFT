package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

const benchmarkHistoryLimit = 50

type benchmarkHistoryFile struct {
	Runs []benchmarkJob `json:"runs"`
}

func loadBenchmarkHistory(
	path string,
) ([]benchmarkJob, error) {
	data, err :=
		os.ReadFile(path)

	if err != nil {
		if errors.Is(
			err,
			os.ErrNotExist,
		) {
			return []benchmarkJob{}, nil
		}

		return nil, err
	}

	if len(data) == 0 {
		return []benchmarkJob{}, nil
	}

	var file benchmarkHistoryFile

	if err :=
		json.Unmarshal(
			data,
			&file,
		); err != nil {
		return nil, err
	}

	if len(file.Runs) >
		benchmarkHistoryLimit {

		file.Runs =
			file.Runs[:benchmarkHistoryLimit]
	}

	if file.Runs == nil {
		file.Runs =
			[]benchmarkJob{}
	}

	return file.Runs, nil
}

func saveBenchmarkHistory(
	path string,
	runs []benchmarkJob,
) error {
	if len(runs) >
		benchmarkHistoryLimit {

		runs =
			runs[:benchmarkHistoryLimit]
	}

	file := benchmarkHistoryFile{
		Runs: runs,
	}

	data, err :=
		json.MarshalIndent(
			file,
			"",
			"  ",
		)

	if err != nil {
		return err
	}

	data =
		append(
			data,
			'\n',
		)

	directory :=
		filepath.Dir(path)

	if err :=
		os.MkdirAll(
			directory,
			0o755,
		); err != nil {
		return err
	}

	temp :=
		path + ".tmp"

	if err :=
		os.WriteFile(
			temp,
			data,
			0o644,
		); err != nil {
		return err
	}

	return os.Rename(
		temp,
		path,
	)
}
