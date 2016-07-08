package datastore

import (
	"errors"
	"fmt"
	"reflect"

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

type Datastore interface {
	Get([]Key, interface{}) error
	Put([]Key, interface{}) ([]Key, error)
	Delete([]Key) error

	AllocateKeys(Key, int) ([]Key, error)

	Run(Query) (Iterator, error)
}

type TransactionalDatastore interface {
	Datastore
	RunInTransaction(func(Datastore) error) error
}

type Iterator interface {
	Next(interface{}) (Key, error)
}

type FilterOp int

const (
	EqualOp FilterOp = iota
	LessThanOp
	LessThanEqualOp
	GreaterThanOp
	GreaterThanEqualOp
)

type Filter struct {
	Name  string
	Value interface{}
	Op    FilterOp
}

type OrderDir int

const (
	AscDir OrderDir = iota
	DescDir
)

type Order struct {
	Name string
	Dir  OrderDir
}

type Query struct {
	Namespace string
	Kind      string
	Filters   []Filter
	Orders    []Order
	KeysOnly  bool
}

type datastore struct {
	ctx context.Context
}

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

func (ds *datastore) propertyListToValue(pl aeds.PropertyList,
	value reflect.Value) {
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}

	value = reflect.Indirect(value) // Make sure the value is a struct.

	for _, p := range pl {
		f := value.FieldByName(p.Name)

		// Check if the field actually exists.
		if !f.IsValid() {
			continue
		}

		fieldValue := p.Value

		aeKey, ok := p.Value.(*aeds.Key)
		if ok {
			fieldValue = ds.toKey(aeKey)
		}

		f.Set(reflect.ValueOf(fieldValue))
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

		// Don't include unexported fields.
		if structField.PkgPath != "" {
			continue
		}

		var propValue interface{}

		// Only include specific field types.
		switch structField.Type.Kind() {
		case reflect.Int64, reflect.String, reflect.Float64:
			propValue = value.Field(i).Interface()
		case reflect.Interface:
			// Check the interface is of Key type.
			key, ok := value.Field(i).Interface().(Key)
			if !ok {
				continue
			}

			aeKey, err := ds.toAEKey(key)
			if err != nil {
				return nil, err
			}
			propValue = aeKey
		default:
			continue
		}

		// TODO: Add indexes, rename and multiple field types.

		pl = append(pl, aeds.Property{
			Name:  structField.Name,
			Value: propValue,
		})

	}
	return pl, nil
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

func (d *datastore) RunInTransaction(f func(Datastore) error) error {
	return aeds.RunInTransaction(d.ctx,
		func(tctx context.Context) error {
			return f(&datastore{
				ctx: tctx,
			})
		}, &aeds.TransactionOptions{XG: true})
}
