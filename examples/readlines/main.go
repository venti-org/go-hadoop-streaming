package main

import (
	"fmt"
	"os"

	mr "github.com/venti-org/go-hadoop-streaming"
)

func main() {
	mr.ReadLines(os.Stdin, func(b []byte, err error) bool {
		if err != nil {
			return false
		}
		fmt.Println(b)
		return true
	})
}
