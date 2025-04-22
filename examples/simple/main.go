package main

import (
	"flag"
	"fmt"
	"os"

	mr "github.com/venti-org/go-hadoop-streaming"
)

type Config struct {
	SkipErr bool
}

type Mapper[K comparable, V any] struct {
	*mr.DefaultMapper[K, V, K, V]
	config *Config
}

func (mapper *Mapper[K, V]) FallbackReadError(err error, ctx *mr.MapperContext[K,
	V, K, V]) error {
	if mapper.config.SkipErr {
		return nil
	}
	return err
}

func (mapper *Mapper[K, V]) Map(key K, value V,
	ctx *mr.MapperContext[K, V, K, V]) error {
	return ctx.Write(key, value)
}

func NewMapper[K comparable, V any](config *Config) *Mapper[K, V] {
	return &Mapper[K, V]{
		DefaultMapper: mr.NewDefaultMapper[K, V, K, V](),
		config:        config,
	}
}

type Reducer[K comparable, V any] struct {
	*mr.DefaultReducer[K, V, K, V]
	config *Config
}

func (reducer *Reducer[K, V]) FallbackReadError(err error, ctx *mr.ReducerContext[K,
	V, K, V]) error {
	if reducer.config.SkipErr {
		return nil
	}
	return err
}

func (reducer *Reducer[K, V]) Reduce(key K, values mr.Iterator[V],
	ctx *mr.ReducerContext[K, V, K, V]) error {
	for values.HasNext() {
		value := values.Next()
		err := ctx.Write(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewReducer[K comparable, V any](config *Config) *Reducer[K, V] {
	return &Reducer[K, V]{
		DefaultReducer: mr.NewDefaultReducer[K, V, K, V](),
		config:         config,
	}
}

func NewMapperRunner[K comparable, V any](config *Config) func() error {
	return func() error {
		mapper := NewMapper[K, V](config)
		ctx := mapper.NewContext(os.Stdin, os.Stdout)
		err := mr.RunMapper(mapper, ctx)
		err2 := ctx.Close()
		return mr.MergeErrors(err, err2)
	}
}

func NewReducerRunner[K comparable, V any](config *Config) func() error {
	return func() error {
		reducer := NewReducer[K, V](config)
		ctx := reducer.NewContext(os.Stdin, os.Stdout)
		err := mr.RunReducer(reducer, ctx)
		err2 := ctx.Close()
		return mr.MergeErrors(err, err2)
	}
}

func main() {
	containKey := flag.Bool("key", false, "")
	typ := flag.String("type", "string", "bool,string,int,uint,float64,complex64,map,array")
	skipErr := flag.Bool("skip-err", false, "")
	reducer := flag.Bool("reducer", false, "")

	flag.Parse()

	config := &Config{
		SkipErr: *skipErr,
	}

	mode := mr.MODE_MAPPER
	if *reducer {
		mode = mr.MODE_REDUCER
	}

	app := mr.NewApplication()
	err := app.WithMapper(func() error {
		if *containKey {
			switch *typ {
			case "bool":
				return NewMapperRunner[string, bool](config)()
			case "string":
				return NewMapperRunner[string, string](config)()
			case "int":
				return NewMapperRunner[string, int](config)()
			case "int8":
				return NewMapperRunner[string, int8](config)()
			case "int16":
				return NewMapperRunner[string, int16](config)()
			case "int32":
				return NewMapperRunner[string, int32](config)()
			case "int64":
				return NewMapperRunner[string, int64](config)()
			case "uint":
				return NewMapperRunner[string, uint](config)()
			case "uint8":
				return NewMapperRunner[string, uint8](config)()
			case "uint16":
				return NewMapperRunner[string, uint16](config)()
			case "uint32":
				return NewMapperRunner[string, uint32](config)()
			case "uint64":
				return NewMapperRunner[string, uint64](config)()
			case "float32":
				return NewMapperRunner[string, float32](config)()
			case "float64":
				return NewMapperRunner[string, float64](config)()
			case "complex64":
				return NewMapperRunner[string, complex64](config)()
			case "complex128":
				return NewMapperRunner[string, complex128](config)()
			case "map":
				return NewMapperRunner[string, map[string]interface{}](config)()
			case "array":
				return NewMapperRunner[string, []interface{}](config)()
			}
		} else {
			switch *typ {
			case "bool":
				return NewMapperRunner[mr.NoneKey, bool](config)()
			case "string":
				return NewMapperRunner[mr.NoneKey, string](config)()
			case "int":
				return NewMapperRunner[mr.NoneKey, int](config)()
			case "int8":
				return NewMapperRunner[mr.NoneKey, int8](config)()
			case "int16":
				return NewMapperRunner[mr.NoneKey, int16](config)()
			case "int32":
				return NewMapperRunner[mr.NoneKey, int32](config)()
			case "int64":
				return NewMapperRunner[mr.NoneKey, int64](config)()
			case "uint":
				return NewMapperRunner[mr.NoneKey, uint](config)()
			case "uint8":
				return NewMapperRunner[mr.NoneKey, uint8](config)()
			case "uint16":
				return NewMapperRunner[mr.NoneKey, uint16](config)()
			case "uint32":
				return NewMapperRunner[mr.NoneKey, uint32](config)()
			case "uint64":
				return NewMapperRunner[mr.NoneKey, uint64](config)()
			case "float32":
				return NewMapperRunner[mr.NoneKey, float32](config)()
			case "float64":
				return NewMapperRunner[mr.NoneKey, float64](config)()
			case "complex64":
				return NewMapperRunner[mr.NoneKey, complex64](config)()
			case "complex128":
				return NewMapperRunner[mr.NoneKey, complex128](config)()
			case "map":
				return NewMapperRunner[mr.NoneKey, map[string]interface{}](config)()
			case "array":
				return NewMapperRunner[mr.NoneKey, []interface{}](config)()
			}
		}
		return fmt.Errorf("not support type %v", *typ)
	}).WithReducer(func() error {
		if *containKey {
			switch *typ {
			case "bool":
				return NewReducerRunner[string, bool](config)()
			case "string":
				return NewReducerRunner[string, string](config)()
			case "int":
				return NewReducerRunner[string, int](config)()
			case "int8":
				return NewReducerRunner[string, int8](config)()
			case "int16":
				return NewReducerRunner[string, int16](config)()
			case "int32":
				return NewReducerRunner[string, int32](config)()
			case "int64":
				return NewReducerRunner[string, int64](config)()
			case "uint":
				return NewReducerRunner[string, uint](config)()
			case "uint8":
				return NewReducerRunner[string, uint8](config)()
			case "uint16":
				return NewReducerRunner[string, uint16](config)()
			case "uint32":
				return NewReducerRunner[string, uint32](config)()
			case "uint64":
				return NewReducerRunner[string, uint64](config)()
			case "float32":
				return NewReducerRunner[string, float32](config)()
			case "float64":
				return NewReducerRunner[string, float64](config)()
			case "complex64":
				return NewReducerRunner[string, complex64](config)()
			case "complex128":
				return NewReducerRunner[string, complex128](config)()
			case "map":
				return NewReducerRunner[string, map[string]interface{}](config)()
			case "array":
				return NewReducerRunner[string, []interface{}](config)()
			}
		} else {
			switch *typ {
			case "bool":
				return NewReducerRunner[mr.NoneKey, bool](config)()
			case "string":
				return NewReducerRunner[mr.NoneKey, string](config)()
			case "int":
				return NewReducerRunner[mr.NoneKey, int](config)()
			case "int8":
				return NewReducerRunner[mr.NoneKey, int8](config)()
			case "int16":
				return NewReducerRunner[mr.NoneKey, int16](config)()
			case "int32":
				return NewReducerRunner[mr.NoneKey, int32](config)()
			case "int64":
				return NewReducerRunner[mr.NoneKey, int64](config)()
			case "uint":
				return NewReducerRunner[mr.NoneKey, uint](config)()
			case "uint8":
				return NewReducerRunner[mr.NoneKey, uint8](config)()
			case "uint16":
				return NewReducerRunner[mr.NoneKey, uint16](config)()
			case "uint32":
				return NewReducerRunner[mr.NoneKey, uint32](config)()
			case "uint64":
				return NewReducerRunner[mr.NoneKey, uint64](config)()
			case "float32":
				return NewReducerRunner[mr.NoneKey, float32](config)()
			case "float64":
				return NewReducerRunner[mr.NoneKey, float64](config)()
			case "complex64":
				return NewReducerRunner[mr.NoneKey, complex64](config)()
			case "complex128":
				return NewReducerRunner[mr.NoneKey, complex128](config)()
			case "map":
				return NewReducerRunner[mr.NoneKey, map[string]interface{}](config)()
			case "array":
				return NewReducerRunner[mr.NoneKey, []interface{}](config)()
			}
		}
		return fmt.Errorf("not support type %v", *typ)
	}).Run(mode)

	if err != nil {
		println(err.Error())
	}
}
