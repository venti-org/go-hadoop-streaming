package hadoop_streaming

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
)

type Counter interface {
	Increment(amount int)
}

type DefaultCounter struct {
	group   string
	counter string
}

func NewDefaultCounter(group, counter string) *DefaultCounter {
	return &DefaultCounter{
		group:   group,
		counter: counter,
	}
}

func (counter *DefaultCounter) Increment(amount int) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%v,%v,%v\n", counter.group, counter.counter, amount)
}

type Context[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
	reader             *bufio.Reader
	readEnd            bool
	writer             *bufio.Writer
	noKeyIn            bool
	noKeyOut           bool
	keyInSerializer    Serializer[KEYIN]
	valueInSerializer  Serializer[VALUEIN]
	keyOutSerializer   Serializer[KEYOUT]
	valueOutSerializer Serializer[VALUEOUT]
	key                KEYIN
}

func NewContext[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any](
	r io.Reader, w io.Writer) *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	var keyIn KEYIN
	var keyOut KEYOUT
	var noneKey NoneKey
	return &Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{
		reader:             bufio.NewReader(r),
		writer:             bufio.NewWriter(w),
		noKeyIn:            reflect.TypeOf(keyIn) == reflect.TypeOf(noneKey),
		noKeyOut:           reflect.TypeOf(keyOut) == reflect.TypeOf(noneKey),
		keyInSerializer:    NewSerializer[KEYIN](),
		valueInSerializer:  NewSerializer[VALUEIN](),
		keyOutSerializer:   NewSerializer[KEYOUT](),
		valueOutSerializer: NewSerializer[VALUEOUT](),
		key:                keyIn,
	}
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) WithKeyInSerializer(
	serializer Serializer[KEYIN]) *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	ctx.keyInSerializer = serializer
	return ctx
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) WithValueInSerializer(
	serializer Serializer[VALUEIN]) *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	ctx.valueInSerializer = serializer
	return ctx
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) WithKeyOutSerializer(
	serializer Serializer[KEYOUT]) *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	ctx.keyOutSerializer = serializer
	return ctx
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) WithValueOutSerializer(
	serializer Serializer[VALUEOUT]) *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	ctx.valueOutSerializer = serializer
	return ctx
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) GetCurrentKey() KEYIN {
	return ctx.key
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) readline() ([]byte, error) {
	if ctx.readEnd {
		return nil, io.EOF
	}
	data, err := ctx.reader.ReadBytes('\n')
	dataLen := len(data)
	if dataLen != 0 && data[dataLen-1] == '\n' {
		data = data[:dataLen-1]
	}
	if err != nil {
		if err == io.EOF {
			ctx.readEnd = true
			if len(data) == 0 {
				return nil, err
			}
			return data, nil
		}
	}
	return data, err
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) readKeyValue() (KEYIN, VALUEIN, error) {
	var err error
	var data []byte
	var key KEYIN
	var value VALUEIN
	for {
		data, err = ctx.readline()
		if err != nil {
			return key, value, err
		}
		if len(data) != 0 {
			break
		}
	}
	if ctx.noKeyIn {
		value, err = ctx.valueInSerializer.Deserialize(data)
	} else {
		items := bytes.SplitN(data, []byte{'\t'}, 2)
		if key, err = ctx.keyInSerializer.Deserialize(items[0]); err != nil {
			return key, value, err
		}
		value, err = ctx.valueInSerializer.Deserialize(items[1])
	}
	return key, value, err
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) GetCounter(group, counter string) Counter {
	return NewDefaultCounter(group, counter)
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) SetStatus(msg string) {
	fmt.Fprintf(os.Stderr, "reporter:status:%vn", msg)
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Check() error {
	if ctx.keyInSerializer == nil && !ctx.noKeyIn {
		return fmt.Errorf("key in serializer is nil")
	}
	if ctx.valueInSerializer == nil {
		return fmt.Errorf("value in serializer is nil")
	}
	if ctx.keyOutSerializer == nil && !ctx.noKeyOut {
		return fmt.Errorf("key out serializer is nil")
	}
	if ctx.valueOutSerializer == nil {
		return fmt.Errorf("value out serializer is nil")
	}
	return nil
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Write(key KEYOUT, value VALUEOUT) error {
	var keyData []byte
	if !ctx.noKeyOut {
		var err error
		keyData, err = ctx.keyOutSerializer.Serialize(key)
		if err != nil {
			return err
		}
	}
	valueData, err := ctx.valueOutSerializer.Serialize(value)
	if err != nil {
		return err
	}
	if len(keyData) != 0 {
		if _, err := ctx.writer.Write(keyData); err != nil {
			return err
		}
		if err := ctx.writer.WriteByte('\t'); err != nil {
			return err
		}
	}
	if _, err := ctx.writer.Write(valueData); err != nil {
		return err
	}
	return ctx.writer.WriteByte('\n')
}

func (ctx *Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Close() error {
	return ctx.writer.Flush()
}

type NoneKey struct{}
