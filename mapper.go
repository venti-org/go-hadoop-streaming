package hadoop_streaming

import (
	"io"
)

type Mapper[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] interface {
	Setup(ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	Map(key KEYIN, value VALUEIN, ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	Cleanup(ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	FallbackReadError(err error, ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
}

type DefaultMapper[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
}

func NewDefaultMapper[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any]() *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	return &DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{}
}

func (mapper *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Setup(
	ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (mapper *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Cleanup(
	ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (mapper *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Map(
	key KEYIN, value VALUEIN, ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (mapper *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) FallbackReadError(
	err error, ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return err
}

func (mapper *DefaultMapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) NewContext(
	r io.Reader, w io.Writer) *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	return NewMapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT](r, w)
}

func RunMapper[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any](
	mapper Mapper[KEYIN, VALUEIN, KEYOUT, VALUEOUT],
	ctx *MapperContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	if err := ctx.Check(); err != nil {
		return err
	}
	err := mapper.Setup(ctx)
	if err != nil {
		return err
	}
	for {
		var ok bool
		ok, err = ctx.NextKeyValue()
		if err != nil {
			err = mapper.FallbackReadError(err, ctx)
			if err == nil {
				continue
			}
		}
		if !ok {
			break
		}
		err = mapper.Map(ctx.GetCurrentKey(), ctx.GetCurrentValue(), ctx)
		if err != nil {
			break
		}
	}
	err2 := mapper.Cleanup(ctx)
	return MergeErrors(err, err2)
}
