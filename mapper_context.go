package hadoop_streaming

import "io"

type MapperContext[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
	*Context[KEYIN, VALUEIN, KEYOUT, VALUEOUT]
	value VALUEIN
}

func NewMapperContext[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any](
	r io.Reader, w io.Writer) *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	var value VALUEIN
	return &MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{
		Context: NewContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT](r, w),
		value:   value,
	}
}

func (ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) NextKeyValue() (bool, error) {
	key, value, err := ctx.readKeyValue()
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	ctx.key = key
	ctx.value = value

	return true, nil
}

func (ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) GetCurrentKey() KEYIN {
	return ctx.key
}

func (ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) GetCurrentValue() VALUEIN {
	return ctx.value
}
