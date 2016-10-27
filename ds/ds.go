package ds

import (
	"errors"

	"golang.org/x/net/context"
)

type IDType int

const (
	StringIDType IDType = iota
	IntIDType
)

type ID interface {
	Type() IDType
}

type StringID string

func (sid StringID) Type() IDType {
	return StringIDType
}

type IntID int64

func (iid IntID) Type() IDType {
	return IntIDType
}

type Key struct {
	Namespace string
	Kind      string
	IDs       []ID
}

type Entity interface {
	Key() Key
}

type Ds interface {
	Get(entities []Entity) error

	Put(entities []Entity) error

	Delete(keys []Key) error

	AllocateKeys(parent Key, n int) ([]Key, error)

	Run(q Query) (Iterator, error)

	RunInTransaction(f func(context.Context) error) error
}

// Iterator is used to get entities from the datastore. A new instance can be
// created by calling Run from the Datastore service.
type Iterator interface {

	// Next returns the next entity and key pair from the iterator. Unlike the
	// official google.golang.org/appengine/datastore.Iterator implementation,
	// the returned key will be nil to signify no more iterables to return.
	Next(entity interface{}) (Key, error)
}

type ConditionType int

const (
	FilterCondition ConditionType = iota
	OrderCondition
)

type Condition interface {
	Type() ConditionType
}

// FilterOp is a type that describes one of the datastore filter comparators
// that can be used when querying for entites by property name and value.
type FilterOp int

const (
	// EqualOp is equivalent to = on the offical App Engine API.
	EqualOp FilterOp = iota

	// LessThanOp is equivalent to < on the official App Engine API.
	LessThanOp

	// LessThanEqualOp is equivalent to <= on the official App Engine API.
	LessThanEqualOp

	// GreaterThanOp is equivalent to > on the official App Engine API.
	GreaterThanOp

	// GreaterThanEqualOp is equivalent to >= on the official App Engine API.
	GreaterThanEqualOp
)

// Filter is used to describe a filter when querying entity properties.
type Filter struct {
	Name  string
	Op    FilterOp
	Value interface{}
}

func (f *Filter) Type() ConditionType {
	return FilterCondition
}

// OrderDir is used to describe which to return results from datastore queries.
type OrderDir int

const (
	// AscDir orders entites from smallest to largest.
	AscDir OrderDir = iota

	// DescDir orders entities from largest to smallest.
	DescDir
)

// Order is used to describe an order on an entity property when querying the
// datastore.
type Order struct {
	Name string
	Dir  OrderDir
}

func (o *Order) Type() ConditionType {
	return OrderCondition
}

// KeyName is the special name given to the key property of an entity.
// Using this as the name in query orders or filters will apply the operation
// to the entity key, not one of its properties.
const KeyName = "__key__"

// Query is used to construct a datastore query.
type Query struct {

	// Namespace is the namespace this query will operate in.
	Namespace string

	// Kind is the entity kind this query will operate on. An empty kind will
	// operate on all entities within an entity group.
	Kind string

	Ancestor Key

	KeysOnly bool

	Conditions []Condition
}

type contextKey int

const (
	dsSliceKey contextKey = iota
)

func AddDs(ctx context.Context, ds Ds) context.Context {
	dsSlice, exists := ctx.Value(dsSliceKey).(*[]Ds)
	if exists {
		*dsSlice = append(*dsSlice, ds)
		return ctx
	}

	return context.WithValue(ctx, dsSliceKey, &[]Ds{ds})
}

func RemoveDs(ctx context.Context, ds Ds) {
	dsSlice, exists := ctx.Value(dsSliceKey).(*[]Ds)
	if exists {
		for i := range *dsSlice {
			if (*dsSlice)[i] == ds {
				// Done this way to prevent leakage of garbage.
				copy((*dsSlice)[i:], (*dsSlice)[i+1:])
				(*dsSlice)[len(*dsSlice)-1] = nil
				*dsSlice = (*dsSlice)[:len(*dsSlice)-1]
				break
			}
		}
	}
}

func loopDs(ctx context.Context, f func(ds Ds) error) error {
	dsSlice, exists := ctx.Value(dsSliceKey).(*[]Ds)
	if exists {
		for _, ds := range *dsSlice {
			if err := f(ds); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("no implementation")
}

func Get(ctx context.Context, entities []Entity) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.Get(entities)
	})
}

func Put(ctx context.Context, entities []Entity) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.Put(entities)
	})
}

func Delete(ctx context.Context, keys []Key) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.Delete(keys)
	})
}

func AllocateKeys(ctx context.Context, parent Key, n int) ([]Key, error) {
	var keys []Key
	return keys, loopDs(ctx, func(ds Ds) error {
		var err error
		keys, err = ds.AllocateKeys(parent, n)
		return err
	})
}

func Run(ctx context.Context, q Query) (Iterator, error) {
	var iter Iterator
	return iter, loopDs(ctx, func(ds Ds) error {
		var err error
		iter, err = ds.Run(q)
		return err
	})
}

func RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.RunInTransaction(f)
	})
}
