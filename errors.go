package main

import "errors"

var (
	ErrNotNumber        = errors.New("Data is not numeric")
	ErrInvalidColumn    = errors.New("InvalidColumnNumber")
	ErrNoFiles          = errors.New("No input files")
	ErrInvalidOperation = errors.New("Invalid operation")
)
