package hadoop_streaming

import (
	"io"
)

type Reducer[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] interface {
	Setup(ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	Reduce(key KEYIN, values Iterator[VALUEIN],
		ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	Cleanup(ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
	FallbackReadError(err error, ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error
}

type DefaultReducer[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any] struct {
}

func NewDefaultReducer[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any]() *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	return &DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]{}
}

func (reducer *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Setup(
	ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (reducer *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Cleanup(
	ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (reducer *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) Reduce(
	key KEYIN, values Iterator[VALUEIN], ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return nil
}

func (reducer *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) FallbackReadError(
	err error, ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	return err
}

func (reducer *DefaultReducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) NewContext(
	r io.Reader, w io.Writer) *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT] {
	return NewReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT](r, w)
}

func RunReducer[KEYIN comparable, VALUEIN, KEYOUT, VALUEOUT any](
	reducer Reducer[KEYIN, VALUEIN, KEYOUT, VALUEOUT],
	ctx *ReducerContext[KEYIN, VALUEIN, KEYOUT, VALUEOUT]) error {
	if err := ctx.Check(); err != nil {
		return err
	}
	err := reducer.Setup(ctx)
	if err != nil {
		return err
	}
	for {
		var ok bool
		ok, err = ctx.NextKeyValue()
		if err != nil {
			err = reducer.FallbackReadError(err, ctx)
			if err == nil {
				ctx.Reset()
				continue
			}
		}
		if !ok {
			break
		}
		err = reducer.Reduce(ctx.GetCurrentKey(), ctx.GetValues(), ctx)
		if err != nil {
			break
		}
	}
	err2 := reducer.Cleanup(ctx)
	return MergeErrors(err, err2)
}
