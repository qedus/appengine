package ds

import (
	"errors"
	"fmt"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"

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
	RootKey Key

	KeysOnly bool

	Orders []Order

	Filters []Filter
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

func ListDs(ctx context.Context) []Ds {
	return *ctx.Value(dsSliceKey).(*[]Ds)
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

type DefaultDs struct {
	GetFunc func(context.Context, []*datastore.Key, interface{}) error
	PutFunc func(context.Context, []*datastore.Key, interface{}) (
		[]*datastore.Key, error)
	DeleteFunc           func(context.Context, []*datastore.Key) error
	RunInTransactionFunc func(context.Context,
		func(context.Context) error, *datastore.TransactionOptions) error
}

func NewDs() Ds {
	return &DefaultDs{
		PutFunc:              datastore.PutMulti,
		DeleteFunc:           datastore.DeleteMulti,
		RunInTransactionFunc: datastore.RunInTransaction,
		GetFunc:              datastore.GetMulti,
	}
}

func (dds *DefaultDs) Get(ctx context.Context,
	keys []Key, entities interface{}) error {

	aeKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		aeKey, err := keyToAEKey(ctx, key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}

	// Map App Engine errors.
	switch err := dds.GetFunc(ctx, aeKeys, entities).(type) {
	case nil:
		return nil
	case appengine.MultiError:
		me := make(Error, len(err))
		for i, ie := range err {
			if ie == datastore.ErrNoSuchEntity {
				me[i] = ErrNoEntity
			} else {
				me[i] = ie
			}
		}
		return me
	default:
		return err
	}
}

func aeKeyToKey(aeKey *datastore.Key) Key {
	aeKeys := make([]*datastore.Key, 0, 1)

	key := Key{
		Namespace: aeKey.Namespace(),
	}

	for {
		aeKeys = append(aeKeys, aeKey)
		aeKey = aeKey.Parent()
		if aeKey == nil {
			break
		}
	}
	for i := len(aeKeys) - 1; i >= 0; i-- {
		aeKey := aeKeys[i]

		key.Path = append(key.Path, struct {
			Kind string
			ID   interface{}
		}{
			aeKey.Kind(),
			nil,
		})

		if aeKey.Incomplete() {
			continue
		} else if aeKey.IntID() != 0 {
			key.Path[i].ID = aeKey.IntID()
		} else {
			key.Path[i].ID = aeKey.StringID()
		}
	}
	return key
}

func keyToAEKey(ctx context.Context, key Key) (*datastore.Key, error) {

	ctx, err := appengine.Namespace(ctx, key.Namespace)
	if err != nil {
		return nil, err
	}

	var aeKey *datastore.Key
	for i, e := range key.Path {
		var parentAEKey *datastore.Key
		if i > 0 {
			parentAEKey = aeKey
		}

		switch ID := e.ID.(type) {
		case string:
			aeKey = datastore.NewKey(ctx, e.Kind, ID, 0, parentAEKey)
		case int64:
			aeKey = datastore.NewKey(ctx, e.Kind, "", ID, parentAEKey)
		case int:
			aeKey = datastore.NewKey(ctx, e.Kind, "", int64(ID), parentAEKey)
		case nil:
			aeKey = datastore.NewIncompleteKey(ctx, e.Kind, parentAEKey)
		default:
			return nil, fmt.Errorf("unknown key ID type %T", ID)
		}
	}
	return aeKey, nil
}

func (dds *DefaultDs) Put(ctx context.Context,
	keys []Key, entities interface{}) ([]Key, error) {
	aeKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		aeKey, err := keyToAEKey(ctx, key)
		if err != nil {
			return nil, err
		}
		aeKeys[i] = aeKey
	}

	completeAEKeys, err := dds.PutFunc(ctx, aeKeys, entities)
	if err != nil {
		return nil, err
	}

	completeKeys := make([]Key, len(completeAEKeys))
	for i, completeAEKey := range completeAEKeys {
		completeKeys[i] = aeKeyToKey(completeAEKey)
	}
	return completeKeys, nil
}

func (dds *DefaultDs) Delete(ctx context.Context, keys []Key) error {
	aeKeys := make([]*datastore.Key, len(keys))
	for i, key := range keys {
		aeKey, err := keyToAEKey(ctx, key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}
	return dds.DeleteFunc(ctx, aeKeys)
}

func (dds *DefaultDs) AllocateKeys(ctx context.Context, key Key, n int) (
	[]Key, error) {

	var parentAEKey *datastore.Key
	if len(key.Path) > 1 {
		parentKey := Key{
			Namespace: key.Namespace,
			Path:      key.Path[:len(key.Path)-1],
		}

		var err error
		parentAEKey, err = keyToAEKey(ctx, parentKey)
		if err != nil {
			return nil, err
		}
	}

	ctx, err := appengine.Namespace(ctx, key.Namespace)
	if err != nil {
		return nil, err
	}

	childElemIndex := len(key.Path) - 1
	kind := key.Path[childElemIndex].Kind
	low, _, err := datastore.AllocateIDs(ctx, kind, parentAEKey, n)
	if err != nil {
		return nil, err
	}

	keys := make([]Key, n)
	for i := range keys {
		keys[i] = key
		keys[i].Path[childElemIndex].ID = low + int64(i)
	}
	return keys, nil
}

func (dds *DefaultDs) Run(ctx context.Context, q Query) (Iterator, error) {
	keyPath := q.RootKey.Path
	aeQ := datastore.NewQuery(keyPath[len(keyPath)-1].Kind)

	if len(keyPath) > 1 {
		ancestorKey := Key{
			Namespace: q.RootKey.Namespace,
			Path:      q.RootKey.Path[:len(q.RootKey.Path)-1],
		}
		ancestorAEKey, err := keyToAEKey(ctx, ancestorKey)
		if err != nil {
			return nil, err
		}
		aeQ = aeQ.Ancestor(ancestorAEKey)
	}

	if q.KeysOnly {
		aeQ = aeQ.KeysOnly()
	}

	for _, order := range q.Orders {
		aeQ = aeQ.Order(string(order.Dir) + order.Name)
	}

	for _, filter := range q.Filters {
		aeQ = aeQ.Filter(filter.Name+string(filter.Op), filter.Value)
	}

	ctx, err := appengine.Namespace(ctx, q.RootKey.Namespace)
	if err != nil {
		return nil, err
	}

	return &defaultIterator{
		iter: aeQ.Run(ctx),
	}, nil
}

type defaultIterator struct {
	iter *datastore.Iterator
}

func (di *defaultIterator) Next(entity interface{}) (Key, error) {
	aeKey, err := di.iter.Next(entity)
	if err == datastore.Done {
		return Key{}, nil
	} else if err != nil {
		return Key{}, err
	}
	return aeKeyToKey(aeKey), nil
}

func (dds *DefaultDs) RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {

	dsSlice := ListDs(ctx)

	return dds.RunInTransactionFunc(ctx,
		func(ctx context.Context) error {
			for _, ds := range dsSlice {
				ctx = AddDs(ctx, ds)
			}
			return f(ctx)
		},
		&datastore.TransactionOptions{
			XG: true,
		})
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

func Get(ctx context.Context, keys []Key, entities interface{}) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.Get(ctx, keys, entities)
	})
}

func Put(ctx context.Context, keys []Key, entities interface{}) ([]Key, error) {
	return keys, loopDs(ctx, func(ds Ds) error {
		var err error
		keys, err = ds.Put(ctx, keys, entities)
		return err
	})
}

func Delete(ctx context.Context, keys []Key) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.Delete(ctx, keys)
	})
}

func AllocateKeys(ctx context.Context, parent Key, n int) ([]Key, error) {
	var keys []Key
	return keys, loopDs(ctx, func(ds Ds) error {
		var err error
		keys, err = ds.AllocateKeys(ctx, parent, n)
		return err
	})
}

func Run(ctx context.Context, q Query) (Iterator, error) {
	var iter Iterator
	return iter, loopDs(ctx, func(ds Ds) error {
		var err error
		iter, err = ds.Run(ctx, q)
		return err
	})
}

func RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	return loopDs(ctx, func(ds Ds) error {
		return ds.RunInTransaction(ctx, f)
	})
}
