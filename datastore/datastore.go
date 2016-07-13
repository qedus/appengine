package datastore

import (
	"fmt"

	"golang.org/x/net/context"
)

// Key represents an App Engine datastore key. Equivalent to that of the
// official package.
type Key interface {
	Namespace() string
	Parent() Key
	Kind() string
	ID() interface{}

	StringID(string, string) Key
	IntID(string, int64) Key

	IncompleteID(string) Key
	Incomplete() bool

	Equal(Key) bool
}

type kindID struct {
	kind string
	id   interface{}
}

func (kid kindID) String() string {
	return fmt.Sprintf("[%s %T:%v]", kid.kind, kid.id, kid.id)
}

type key struct {
	namespace string
	kindIDs   []kindID
}

func (k key) String() string {
	return fmt.Sprintf("Key: ns: %s path: %s", k.namespace, k.kindIDs)
}

func (k *key) Namespace() string {
	return k.namespace
}

func (k *key) Parent() Key {
	if len(k.kindIDs) <= 1 {
		return nil
	}

	kindIDs := make([]kindID, len(k.kindIDs)-1)
	copy(kindIDs, k.kindIDs) // Will not copy the leaf kindID element.
	return &key{
		namespace: k.namespace,
		kindIDs:   kindIDs,
	}
}

func (k *key) Kind() string {
	return k.kindIDs[len(k.kindIDs)-1].kind
}

func (k *key) ID() interface{} {
	return k.kindIDs[len(k.kindIDs)-1].id
}

func (k *key) Equal(o Key) bool {
	if k == nil && o == nil {
		return true
	} else if k == nil || o == nil {
		return false
	}

	r, ok := o.(*key)
	if !ok {
		return false
	}

	if k.namespace != r.namespace {
		return false
	}
	if len(k.kindIDs) != len(r.kindIDs) {
		return false
	}
	for i, ki := range k.kindIDs {
		if ki.kind != r.kindIDs[i].kind {
			return false
		}

		if ki.id != r.kindIDs[i].id {
			return false
		}
	}
	return true
}

// NewKey creates a new key with the specified namespace. Any key derived from
// this key via StringID, IntID or IncompleteID will inherit the namespace.
func NewKey(namespace string) Key {
	return &key{
		namespace: namespace,
	}
}

func (k *key) newChild(kind string, id interface{}) Key {
	kindIDs := make([]kindID, len(k.kindIDs), len(k.kindIDs)+1)
	copy(kindIDs, k.kindIDs)
	kindIDs = append(kindIDs, kindID{
		kind: kind,
		id:   id,
	})
	return &key{
		namespace: k.namespace,
		kindIDs:   kindIDs,
	}

}

func (k *key) IntID(kind string, id int64) Key {
	return k.newChild(kind, id)
}

func (k *key) StringID(kind, id string) Key {
	return k.newChild(kind, id)
}

func (k *key) IncompleteID(kind string) Key {
	return k.newChild(kind, nil)
}

func (k *key) Incomplete() bool {
	return k.ID() == nil
}

// Datastore represents the functionality that is offered by the App Engine
// datastore service within a transaction. TransactionalDatastore is the service
// initialised with the New function and differes from the Datastore interface
// as it allows transactions to be created.
type Datastore interface {

	// Get populates a slice of entities if available using the specified
	// complete string or integer keys. It is the GetMulti equivalent in the
	// official datastore package. Entities can be any []S or []*S where S is a
	// struct. If an entity cannot be found then an error will be returned with
	// method signature NotFound(index int) bool where index is the key/entity
	// index that is being being checked for presence.
	Get(keys []Key, entities interface{}) error

	// Put saves entities to the datastore. Complete or incomplete string or
	// integer keys can be used. The returned keys are complete keys. Entities
	// can be any []S or []*S where S is a struct.
	Put(keys []Key, entities interface{}) ([]Key, error)

	// Delete deletes entities with the specified string or integer complete
	// keys.
	Delete(keys []Key) error

	// AllocateKeys returns n unique integer keys based on the key inputs kind
	// and parents. Therefore key can be complete or incomplete as the ID part
	// is disregarded.
	AllocateKeys(key Key, n int) ([]Key, error)

	// Run runs a query against the datastore and returns an iterator.
	Run(q Query) (Iterator, error)
}

// TransactionalDatastore represents an App Engine datastore service that allows
// transactions to be created with RunInTransaction.
type TransactionalDatastore interface {
	Datastore

	// RunInTransaction ensures all datastore mutation operations run within
	// function f using ds to be run atomically. Up to twenty five entities
	// and/or entity groups can be mutated at a time.
	RunInTransaction(f func(ds Datastore) error) error
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

	Filters []Filter
	Orders  []Order

	KeysOnly bool
}

var datastoreKey = "datastore"

// WithContext is a convienience function to add a TransactionalDatastore to a
// context.
func WithContext(parent context.Context,
	ds TransactionalDatastore) context.Context {
	return context.WithValue(parent, &datastoreKey, ds)
}

// FromContext is a convenience function to retrieve a TransactionalDatastore
// from a context that had previously been operated on with WithContext.
func FromContext(ctx context.Context) (TransactionalDatastore, bool) {
	ds, ok := ctx.Value(&datastoreKey).(TransactionalDatastore)
	return ds, ok
}
