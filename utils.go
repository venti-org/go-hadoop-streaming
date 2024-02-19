package hadoop_streaming

import (
	"bufio"
	"io"
	"strings"
	"unsafe"
)

func SizeofBits[T any]() int {
	var empty T
	return int(unsafe.Sizeof(empty) * 8)
}

func ReadLines(r io.Reader, callback func(string, error) bool) {
	readLines(r, callback, false)
}

func ReadLinesAndTrim(r io.Reader, callback func(string, error) bool) {
	readLines(r, callback, true)
}

func readLines(r io.Reader, callback func(string, error) bool, trimSpace bool) {
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if trimSpace {
			line = strings.TrimSpace(line)
		}
		if !callback(line, err) {
			break
		}
	}
}

type MultiError struct {
	errs []error
}

func (me *MultiError) Error() string {
	var msg strings.Builder
	msg.WriteString("Multiple errors occurred:")
	for _, err := range me.errs {
		msg.WriteString("\n - ")
		msg.WriteString(err.Error())
	}
	return msg.String()
}

func (me *MultiError) Errs() []error {
	return me.errs
}

func (me *MultiError) AutoConvert() error {
	errs := me.errs
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return me
}

func (me *MultiError) AddErrs(errs ...error) {
	for _, err := range errs {
		if err == nil {
			continue
		}
		if other, ok := err.(*MultiError); ok {
			me.errs = append(me.errs, other.Errs()...)
		} else {
			me.errs = append(me.errs, err)
		}
	}
}

func MergeErrors(errs ...error) error {
	err := &MultiError{}
	err.AddErrs(errs...)
	return err.AutoConvert()
}
