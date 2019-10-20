package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/reinerRubin/cyclosa"
)

type args struct {
	inputFilename     string
	outputFilename    string
	keyLimit          int
	tempFilesQuantity int
	removeTempFiles   bool
}

func parseArgs() (*args, error) {
	args := &args{}
	flag.IntVar(&args.keyLimit, "keylimit", 100, "max uniq keys in memory")
	flag.IntVar(&args.tempFilesQuantity, "tempfilesquantity", 10, "how many temporary files to use")
	flag.BoolVar(&args.removeTempFiles, "removetempfiles", true, "remove temp files")

	flag.Parse()

	if args.tempFilesQuantity == 0 {
		args.tempFilesQuantity = args.keyLimit
	}

	inputFilename := flag.Arg(0)
	if inputFilename == "" {
		return nil, errors.New("no input filename")
	}
	args.inputFilename = inputFilename

	outputFilename := flag.Arg(1)
	if outputFilename == "" {
		return nil, errors.New("no output filename")
	}
	args.outputFilename = outputFilename

	return args, nil
}

func app() error {
	args, err := parseArgs()
	if err != nil {
		return fmt.Errorf("cant parse args: %s", err)
	}

	err = cyclosa.Run(&cyclosa.AppSettings{
		InputFilename:   args.inputFilename,
		OutputFilename:  args.outputFilename,
		KeyLimit:        uint(args.keyLimit),
		SectorsQuantity: uint(args.tempFilesQuantity),
		RemoveTempFiles: args.removeTempFiles,
	})
	if err != nil {
		return fmt.Errorf("main process failed: %s", err)
	}

	return nil
}

func main() {
	if err := app(); err != nil {
		fmt.Printf("exit with err: %s\n", err)
	}
}
