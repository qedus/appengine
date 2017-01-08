package ds

import (
	"errors"

	"golang.org/x/net/context"
)

var ErrNoEntity = errors.New("no entity")

type Error []error

func (e Error) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	return "multiple errors"
}

type Key struct {
	Namespace string
	Path      []struct {
		Kind string
		ID   interface{}
	}
}

func NewKey(namespace string) Key {
	return Key{
		Namespace: namespace,
	}
}

func (k Key) Append(kind string, ID interface{}) Key {
	return Key{
		Namespace: k.Namespace,
		Path: append(k.Path, struct {
			Kind string
			ID   interface{}
		}{
			Kind: kind,
			ID:   ID,
		}),
	}
}

func (k Key) Equal(key Key) bool {
	if k.Namespace != key.Namespace {
		return false
	}
	for i, p := range k.Path {
		if p != key.Path[i] {
			return false
		}
	}
	return true
}

type Ds interface {
	Get(context.Context, []Key, interface{}) error

	Put(context.Context, []Key, interface{}) ([]Key, error)

	Delete(context.Context, []Key) error

	AllocateKeys(context.Context, Key, int) ([]Key, error)

	Run(context.Context, Query) (Iterator, error)

	RunInTransaction(context.Context, func(context.Context) error) error
}

// Iterator is used to get entities from the datastore. A new instance can be
// created by calling Run from the Datastore service.
type Iterator interface {

	// Next returns the next entity and key pair from the iterator. Unlike the
	// official google.golang.org/appengine/datastore.Iterator implementation,
	// the returned key will be nil to signify no more iterables to return.
	Next(entity interface{}) (Key, error)
}

// FilterOp is a type that describes one of the datastore filter comparators
// that can be used when querying for entites by property name and value.
type FilterOp string

const (
	// EqualOp is equivalent to = on the offical App Engine API.
	EqualOp FilterOp = "="

	// LessThanOp is equivalent to < on the official App Engine API.
	LessThanOp = "<"

	// LessThanEqualOp is equivalent to <= on the official App Engine API.
	LessThanEqualOp = "<="

	// GreaterThanOp is equivalent to > on the official App Engine API.
	GreaterThanOp = ">"

	// GreaterThanEqualOp is equivalent to >= on the official App Engine API.
	GreaterThanEqualOp = ">="
)

// Filter is used to describe a filter when querying entity properties.
type Filter struct {
	Name  string
	Op    FilterOp
	Value interface{}
}

// OrderDir is used to describe which to return results from datastore queries.
type OrderDir string

const (
	// AscDir orders entites from smallest to largest.
	AscDir OrderDir = ""

	// DescDir orders entities from largest to smallest.
	DescDir = "-"
)

// Order is used to describe an order on an entity property when querying the
// datastore.
type Order struct {
	Name string
	Dir  OrderDir
}

// KeyName is the special name given to the key property of an entity.
// Using this as the name in query orders or filters will apply the operation
// to the entity key, not one of its properties.
const KeyName = "__key__"

// Query is used to construct a datastore query.
type Query struct {
	Root Key

	KeysOnly bool

	Orders []Order

	Filters []Filter
}

func Get(ctx context.Context, keys []Key, entities interface{}) error {
	return fromContext(ctx).Get(ctx, keys, entities)
}

func Put(ctx context.Context, keys []Key, entities interface{}) ([]Key, error) {
	return fromContext(ctx).Put(ctx, keys, entities)
}

func Delete(ctx context.Context, keys []Key) error {
	return fromContext(ctx).Delete(ctx, keys)
}

func AllocateKeys(ctx context.Context, parent Key, n int) ([]Key, error) {
	return fromContext(ctx).AllocateKeys(ctx, parent, n)
}

func Run(ctx context.Context, q Query) (Iterator, error) {
	return fromContext(ctx).Run(ctx, q)
}

func RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	return fromContext(ctx).RunInTransaction(ctx, f)
}

var contextKey = "ds context key"

func NewContext(ctx context.Context, ds Ds) context.Context {
	return context.WithValue(ctx, &contextKey, ds)
}

func fromContext(ctx context.Context) Ds {
	return ctx.Value(&contextKey).(Ds)
}
