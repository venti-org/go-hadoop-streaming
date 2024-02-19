package hadoop_streaming

import (
	"encoding/json"
	"reflect"
	"strconv"
)

type JsonSerializer[T any] struct{}

func (s JsonSerializer[T]) Serialize(from T) ([]byte, error) {
	return json.Marshal(from)
}

func (s JsonSerializer[T]) Deserialize(to []byte) (T, error) {
	var t T
	err := json.Unmarshal(to, &t)
	if err != nil {
		var empty T
		return empty, err
	}
	return t, nil
}

type StringSerializer struct{}

func (s StringSerializer) Serialize(from string) ([]byte, error) {
	return []byte(from), nil
}

func (s StringSerializer) Deserialize(to []byte) (string, error) {
	return string(to), nil
}

type UintSerializer[T uint | uint8 | uint16 | uint32 | uint64] struct{}

func (s UintSerializer[T]) Serialize(from T) ([]byte, error) {
	return []byte(strconv.FormatUint(uint64(from), 10)), nil
}

func (s UintSerializer[T]) Deserialize(to []byte) (T, error) {
	num, err := strconv.ParseUint(string(to), 10, SizeofBits[T]())
	return T(num), err
}

type IntSerializer[T int | int8 | int16 | int32 | int64] struct{}

func (s IntSerializer[T]) Serialize(from T) ([]byte, error) {
	return []byte(strconv.FormatInt(int64(from), 10)), nil
}

func (s IntSerializer[T]) Deserialize(to []byte) (T, error) {
	num, err := strconv.ParseInt(string(to), 10, SizeofBits[T]())
	return T(num), err
}

type FloatSerializer[T float32 | float64] struct{}

func (s FloatSerializer[T]) Serialize(from T) ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(from), 'f', -1, SizeofBits[T]())), nil
}

func (s FloatSerializer[T]) Deserialize(to []byte) (T, error) {
	num, err := strconv.ParseFloat(string(to), SizeofBits[T]())
	return T(num), err
}

type BoolSerializer struct{}

func (s BoolSerializer) Serialize(from bool) ([]byte, error) {
	return []byte(strconv.FormatBool(from)), nil
}

func (s BoolSerializer) Deserialize(to []byte) (bool, error) {
	return strconv.ParseBool(string(to))
}

type ComplexSerializer[T complex64 | complex128] struct{}

func (s ComplexSerializer[T]) Serialize(from T) ([]byte, error) {
	return []byte(strconv.FormatComplex(complex128(from), 'f', -1, SizeofBits[T]())), nil
}

func (s ComplexSerializer[T]) Deserialize(to []byte) (T, error) {
	num, err := strconv.ParseComplex(string(to), SizeofBits[T]())
	return T(num), err
}

type Serializer[T any] interface {
	Deserialize(value []byte) (T, error)
	Serialize(value T) ([]byte, error)
}

func NewSerializer[T any]() Serializer[T] {
	var serializer interface{}
	var to T
	var noneKey NoneKey
	toType := reflect.TypeOf(to)
	toTypeKind := toType.Kind()
	switch toTypeKind {
	case reflect.Bool:
		serializer = &BoolSerializer{}
	case reflect.Float32:
		serializer = &FloatSerializer[float32]{}
	case reflect.Float64:
		serializer = &FloatSerializer[float64]{}
	case reflect.Complex64:
		serializer = &ComplexSerializer[complex64]{}
	case reflect.Complex128:
		serializer = &ComplexSerializer[complex128]{}
	case reflect.Int:
		serializer = &IntSerializer[int]{}
	case reflect.Int8:
		serializer = &IntSerializer[int8]{}
	case reflect.Int16:
		serializer = &IntSerializer[int16]{}
	case reflect.Int32:
		serializer = &IntSerializer[int32]{}
	case reflect.Int64:
		serializer = &IntSerializer[int64]{}
	case reflect.Uint:
		serializer = &UintSerializer[uint]{}
	case reflect.Uint8:
		serializer = &UintSerializer[uint8]{}
	case reflect.Uint16:
		serializer = &UintSerializer[uint16]{}
	case reflect.Uint32:
		serializer = &UintSerializer[uint32]{}
	case reflect.Uint64:
		serializer = &UintSerializer[uint64]{}
	case reflect.String:
		serializer = &StringSerializer{}
	case reflect.Slice:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Struct:
		fallthrough
	case reflect.Ptr:
		if toType != reflect.TypeOf(noneKey) {
			serializer = &JsonSerializer[T]{}
		}
	}
	if serializer == nil {
		return nil
	}
	return serializer.(Serializer[T])
}
