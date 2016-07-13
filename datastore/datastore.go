package datastore

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	ids "github.com/qedus/appengine/internal/datastore"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	aeds "google.golang.org/appengine/datastore"
)

type notFoundError map[int]bool

func (nfe notFoundError) Error() string {
	return "entities not found"
}

func (nfe notFoundError) NotFound(index int) bool {
	return nfe[index]
}

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
		if !reflect.DeepEqual(ki.id, r.kindIDs[i].id) {
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

type datastore struct {
	ctx context.Context
}

// New returns a new TransactionalDatastore service that can be used to interact
// with the App Engine production and development SDK datastores.
func New(ctx context.Context) TransactionalDatastore {
	return &datastore{
		ctx: ctx,
	}
}

func (ds *datastore) toAEKey(key Key) (*aeds.Key, error) {
	// Prevent infinite recursion when key is nil.
	if key == nil {
		return nil, nil
	}

	kind := key.Kind()
	parent, err := ds.toAEKey(key.Parent())
	if err != nil {
		return nil, err
	}

	ctx, err := appengine.Namespace(ds.ctx, key.Namespace())
	if err != nil {
		return nil, err
	}

	switch id := key.ID().(type) {
	case string:
		return aeds.NewKey(ctx, kind, id, 0, parent), nil
	case int64:
		return aeds.NewKey(ctx, kind, "", id, parent), nil
	case nil:
		return aeds.NewIncompleteKey(ctx, kind, parent), nil
	}
	return nil, errors.New("unknown key ID type")
}

func (ds *datastore) toKey(aeKey *aeds.Key) Key {
	namespace := aeKey.Namespace()

	// Collect the entire key path.
	aeKeys := []*aeds.Key{aeKey}
	for {
		aeKey = aeKey.Parent()
		if aeKey == nil {
			break
		}
		aeKeys = append(aeKeys, aeKey)
	}

	// Replay the keys in ancestor order first.
	key := NewKey(namespace)
	for i := len(aeKeys) - 1; i >= 0; i-- {
		aeKey := aeKeys[i]

		if aeKey.StringID() == "" {
			// An int id.
			key = key.IntID(aeKey.Kind(), aeKey.IntID())
		} else {
			// A string id.
			key = key.StringID(aeKey.Kind(), aeKey.StringID())
		}
	}
	return key
}

func (ds *datastore) valueToPropertyList(value reflect.Value) (
	aeds.PropertyList, error) {
	ty := value.Type()

	// Get the underlying type if the value is an interface.
	if ty.Kind() == reflect.Interface {
		value = value.Elem()
	}

	// Make sure we have the struct, not the pointer.
	value = reflect.Indirect(value)
	ty = value.Type()

	pl := make(aeds.PropertyList, 0, ty.NumField())

	for i := 0; i < ty.NumField(); i++ {
		structField := ty.Field(i)

		propName := ids.PropertyName(structField)
		if propName == "" {
			// If there is no name then go on to the next field.
			continue
		}

		// Only include specific field types.
		var propValue interface{}
		switch structField.Type.Kind() {
		case reflect.Int64, reflect.String, reflect.Float64:
			propValue = value.Field(i).Interface()
		case reflect.Struct:
			switch v := value.Field(i).Interface().(type) {
			case time.Time:
				propValue = v
			default:
				continue
			}
		case reflect.Interface:
			// Check the interface is of Key type.
			key, ok := value.Field(i).Interface().(Key)
			if !ok {
				// We currentlly don't allow any other type of interfaces.
				continue
			}

			aeKey, err := ds.toAEKey(key)
			if err != nil {
				return nil, err
			}
			propValue = aeKey
		case reflect.Slice:
			// Only accept certain types of slice.
			switch structField.Type.Elem().Kind() {
			case reflect.Int64, reflect.Float64, reflect.String:

				// Must convert the slice to a slice of properties.
				slice := value.Field(i)
				for i := 0; i < slice.Len(); i++ {
					pl = append(pl, aeds.Property{
						Name:     propName,
						Value:    slice.Index(i).Interface(),
						NoIndex:  ids.PropertyNoIndex(structField),
						Multiple: true,
					})
				}
				continue
			default:
				continue
			}

		default:
			continue
		}

		pl = append(pl, aeds.Property{
			Name:     propName,
			Value:    propValue,
			NoIndex:  ids.PropertyNoIndex(structField),
			Multiple: false,
		})

	}
	return pl, nil
}

func (ds *datastore) propertyListToValue(pl aeds.PropertyList,
	value reflect.Value) {
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}

	value = reflect.Indirect(value) // Make sure the value is a struct.
	valueType := value.Type()

	// Datastore property names are derived from struct field names or custom
	// struct tags. Map any tag renames to actual struct fields in order to get
	// the field value.
	fieldValues := make(map[string]reflect.Value, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := valueType.Field(i)

		propName := ids.PropertyName(field)
		if propName == "" {
			// The struct user doesn't want this field or it is unexported.
			continue
		}
		fieldValues[propName] = value.Field(i)
	}

	multiProps := map[string]reflect.Value{}
	for _, p := range pl {

		// Is there a struct field that can take this property?
		v, exists := fieldValues[p.Name]
		if !exists {
			continue
		}

		if p.Multiple {
			if _, exists := multiProps[p.Name]; !exists {
				sliceType := reflect.SliceOf(reflect.TypeOf(p.Value))
				multiProps[p.Name] = reflect.MakeSlice(sliceType, 0, 1)
			}

			multiProps[p.Name] = reflect.Append(multiProps[p.Name],
				reflect.ValueOf(p.Value))
			continue
		}

		if p.Value == nil {
			continue
		}

		// Do any of the property values need to be transformed.
		propValue := p.Value

		switch v := propValue.(type) {
		case *aeds.Key:
			propValue = ds.toKey(v)
		}

		v.Set(reflect.ValueOf(propValue))
	}

	for propName, propValues := range multiProps {
		fieldValue, exists := fieldValues[propName]
		if !exists {
			continue
		}

		fieldValue.Set(propValues)
	}
}

func (ds *datastore) Get(keys []Key, entities interface{}) error {
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}

	pls := make([]aeds.PropertyList, len(keys))
	switch err := aeds.GetMulti(ds.ctx, aeKeys, pls).(type) {
	case nil:
		values := reflect.ValueOf(entities)
		for i, pl := range pls {
			ds.propertyListToValue(pl, values.Index(i))
		}
		return nil
	case appengine.MultiError:
		nfe := notFoundError{}

		values := reflect.ValueOf(entities)
		for i, pl := range pls {
			switch err[i] {
			case nil:
				ds.propertyListToValue(pl, values.Index(i))
			case aeds.ErrNoSuchEntity:
				nfe[i] = true
			default:
				return err[i]
			}
		}

		return nfe
	default:
		return err
	}
}

func (ds *datastore) Delete(keys []Key) error {
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}
	return aeds.DeleteMulti(ds.ctx, aeKeys)
}

func verifyKeysValues(keys []Key, values reflect.Value) error {
	if values.Kind() != reflect.Slice {
		return errors.New("entities not a slice")
	}

	if len(keys) != values.Len() {
		return errors.New("keys length not same as entities length")
	}

	sliceEntityType := values.Type().Elem()
	switch sliceEntityType.Kind() {
	case reflect.Struct:
		return nil
	case reflect.Ptr:
		if sliceEntityType.Elem().Kind() == reflect.Struct {
			return nil
		}
	case reflect.Interface:
		// Need to check that each value is a struct pointer as per App Engine
		// requirements.
		for i := 0; i < values.Len(); i++ {
			val := values.Index(i)
			// Check the interface value is a pointer to a struct.
			if val.Kind() == reflect.Interface &&
				val.Elem().Kind() == reflect.Ptr &&
				val.Elem().Elem().Kind() == reflect.Struct {
				continue
			}

			// Not a pointer to a struct so raise an error.
			return errors.New(
				"interface slice does not contain struct pointers")
		}
		return nil
	}
	return errors.New("entities not structs or pointers")
}

func (ds *datastore) Put(keys []Key, entities interface{}) ([]Key, error) {

	values := reflect.ValueOf(entities)
	if err := verifyKeysValues(keys, values); err != nil {
		return nil, err
	}

	// Convert keys to App Engine keys.
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return nil, err
		}
		aeKeys[i] = aeKey
	}

	// Convert values to App Engine property lists so we can convert key
	// properties.
	pls := make([]aeds.PropertyList, values.Len())
	for i := 0; i < values.Len(); i++ {
		pl, err := ds.valueToPropertyList(values.Index(i))
		if err != nil {
			return nil, err
		}
		pls[i] = pl
	}

	completeAEKeys, err := aeds.PutMulti(ds.ctx, aeKeys, pls)
	if err != nil {
		return nil, err
	}
	completeKeys := make([]Key, len(completeAEKeys))
	for i, completeAEKey := range completeAEKeys {
		completeKeys[i] = ds.toKey(completeAEKey)
	}
	return completeKeys, nil
}

func (ds *datastore) AllocateKeys(key Key, n int) ([]Key, error) {
	ctx, err := appengine.Namespace(ds.ctx, key.Namespace())
	if err != nil {
		return nil, err
	}
	parentKey, err := ds.toAEKey(key.Parent())
	if err != nil {
		return nil, err
	}

	low, _, err := aeds.AllocateIDs(ctx, key.Kind(), parentKey, n)
	if err != nil {
		return nil, err
	}

	baseKey := key.Parent()
	if baseKey == nil {
		baseKey = NewKey(key.Namespace())
	}

	keys := make([]Key, n)
	for i := range keys {
		keys[i] = baseKey.IntID(key.Kind(), low+int64(i))
	}
	return keys, nil
}

type iterator struct {
	ds   *datastore
	iter *aeds.Iterator
}

func (it *iterator) Next(entity interface{}) (Key, error) {

	pl := aeds.PropertyList{}
	aeKey, err := it.iter.Next(&pl)
	if err == aeds.Done {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// Entity could be nil if keys only queries are used.
	if entity != nil {
		// Currently used to convert datastore.Keys to this packages keys.
		it.ds.propertyListToValue(pl, reflect.ValueOf(entity))
	}

	return it.ds.toKey(aeKey), nil
}

func (ds *datastore) Run(q Query) (Iterator, error) {
	aeQ := aeds.NewQuery(q.Kind)

	if q.Ancestor != nil {
		aeKey, err := ds.toAEKey(q.Ancestor)
		if err != nil {
			return nil, err
		}
		aeQ = aeQ.Ancestor(aeKey)
	}

	if q.KeysOnly {
		aeQ = aeQ.KeysOnly()
	}

	// Apply orders.
	for _, o := range q.Orders {
		var dirStr string
		switch o.Dir {
		case AscDir:
			dirStr = ""
		case DescDir:
			dirStr = "-"
		default:
			return nil, errors.New("unknown order dir")
		}
		aeQ = aeQ.Order(dirStr + o.Name)
	}

	// Apply fiters.
	for _, f := range q.Filters {
		var opStr string
		switch f.Op {
		case LessThanOp:
			opStr = "<"
		case LessThanEqualOp:
			opStr = "<="
		case EqualOp:
			opStr = "="
		case GreaterThanEqualOp:
			opStr = ">="
		case GreaterThanOp:
			opStr = ">"
		default:
			return nil, errors.New("unknown filter op")
		}

		value := f.Value

		// Convert Key values to datastore.Keys.
		if key, ok := value.(Key); ok {
			aeKey, err := ds.toAEKey(key)
			if err != nil {
				panic(err)
			}
			value = aeKey
		}

		aeQ = aeQ.Filter(f.Name+opStr, value)
	}

	ctx, err := appengine.Namespace(ds.ctx, q.Namespace)
	if err != nil {
		return nil, err
	}
	return &iterator{
		ds:   ds,
		iter: aeQ.Run(ctx),
	}, nil
}

func (ds *datastore) RunInTransaction(f func(Datastore) error) error {
	return aeds.RunInTransaction(ds.ctx,
		func(tctx context.Context) error {
			return f(&datastore{
				ctx: tctx,
			})
		}, &aeds.TransactionOptions{XG: true})
}
