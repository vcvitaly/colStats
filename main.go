package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	// Verify and parse arguments
	op := flag.String("op", "sum", "Operation to be executed")
	column := flag.Int("col", 1, "CSV column on which to execute operation")
	flag.Parse()

	fileNames, err := listFiles(flag.Args())
	if err != nil {
		log.Fatalf("An error happened while traversing the input directory: %v", err)
	}
	if err := run(fileNames, *op, *column, os.Stdout); err != nil {
		log.Fatalf("An error while running the main logic: %v", err)
	}
}

func run(filenames []string, op string, column int, out io.Writer) error {
	var opFunc statsFunc

	if len(filenames) == 0 {
		return ErrNoFiles
	}

	if column < 1 {
		return fmt.Errorf("%w: %d", ErrInvalidColumn, column)
	}

	// Validate the operation and define the opFunc accordingly
	switch op {
	case "sum":
		opFunc = sum
	case "avg":
		opFunc = avg
	default:
		return fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	consolidate := make([]float64, 0)

	// Create the channel to receive results or errors of operations
	resCh := make(chan []float64)
	errCh := make(chan error)
	doneCh := make(chan struct{})

	wg := sync.WaitGroup{}

	// Loop through all files and create a goroutine to process
	// each one concurrently
	for _, fname := range filenames {
		wg.Add(1)

		go func(fname string) {
			defer wg.Done()

			f, err := os.Open(fname)
			if err != nil {
				errCh <- fmt.Errorf("Cannot open file: %w", err)
				return
			}

			// Parse the CSV into a slice of float64 numbers
			floats, err := csv2Float(f, column)
			if err != nil {
				errCh <- fmt.Errorf("An error while parsing floats: %w", err)
				return
			}

			if err := f.Close(); err != nil {
				errCh <- fmt.Errorf("An error while closing the file: %w", err)
			}

			resCh <- floats
		}(fname)
	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()

	for {
		select {
		case err := <-errCh:
			return err
		case floats := <-resCh:
			consolidate = append(consolidate, floats...)
		case <-doneCh:
			_, err := fmt.Fprintln(out, opFunc(consolidate))
			return err
		}
	}
}

func listFiles(paths []string) ([]string, error) {
	filenames := make([]string, 0)
	for _, p := range paths {
		err := filepath.Walk(p, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.IsDir() {
				filenames = append(filenames, path)
			}

			return nil
		})

		if err != nil {
			return nil, err
		}
	}
	return filenames, nil
}
