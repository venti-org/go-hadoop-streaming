package hadoop_streaming

import "io"

type Iterator[T any] interface {
	HasNext() bool
	Next() T
}

type ReducerIterator[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
	key   KEYIN
	value *VALUEIN
	ctx   *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]
}

func (iterator *ReducerIterator[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) HasNext() bool {
	if iterator.value != nil {
		return true
	}
	ctx := iterator.ctx
	key, value, err := ctx.readKeyValue()
	if err != nil {
		ctx.err = err
		return false
	}
	valuePtr := &value
	if iterator.key != key {
		ctx.key = key
		ctx.value = valuePtr
		return false
	}
	iterator.value = valuePtr
	return true
}

func (iterator *ReducerIterator[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Next() VALUEIN {
	value := *iterator.value
	iterator.value = nil
	return value
}

type ReducerContext[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
	*Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]
	value *VALUEIN
	err   error
}

func NewReducerContext[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any](
	r io.Reader, w io.Writer) *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	return &ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{
		Context: NewContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT](r, w),
	}
}

func (ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Reset() {
	ctx.err = nil
	ctx.value = nil
}

func (ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) NextKeyValue() (bool, error) {
	if ctx.err != nil {
		return false, nil
	}
	if ctx.value != nil {
		return true, nil
	}
	key, value, err := ctx.readKeyValue()
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	ctx.key = key
	ctx.value = &value

	return true, nil
}

func (ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) GetValues() Iterator[VALUEIN] {
	iterator := &ReducerIterator[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{
		key:   ctx.key,
		value: ctx.value,
		ctx:   ctx,
	}
	ctx.value = nil
	return iterator
}
