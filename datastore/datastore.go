package datastore

import (
	"fmt"

	"github.com/qedus/ds"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
)

func New() ds.Ds {
	return &Ds{
		PutFunc:              datastore.PutMulti,
		DeleteFunc:           datastore.DeleteMulti,
		RunInTransactionFunc: datastore.RunInTransaction,
		GetFunc:              datastore.GetMulti,
	}
}

type Ds struct {
	GetFunc func(context.Context, []*datastore.Key, interface{}) error
	PutFunc func(context.Context, []*datastore.Key, interface{}) (
		[]*datastore.Key, error)
	DeleteFunc           func(context.Context, []*datastore.Key) error
	RunInTransactionFunc func(context.Context,
		func(context.Context) error, *datastore.TransactionOptions) error
}

func (dds *Ds) Get(ctx context.Context,
	keys []ds.Key, entities interface{}) error {

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
		me := make(ds.Error, len(err))
		for i, ie := range err {
			if ie == datastore.ErrNoSuchEntity {
				me[i] = ds.ErrNoEntity
			} else {
				me[i] = ie
			}
		}
		return me
	default:
		return err
	}
}

func aeKeyToKey(aeKey *datastore.Key) ds.Key {
	aeKeys := make([]*datastore.Key, 0, 1)

	key := ds.Key{
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

func keyToAEKey(ctx context.Context, key ds.Key) (*datastore.Key, error) {

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

func (dds *Ds) Put(ctx context.Context,
	keys []ds.Key, entities interface{}) ([]ds.Key, error) {
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

	completeKeys := make([]ds.Key, len(completeAEKeys))
	for i, completeAEKey := range completeAEKeys {
		completeKeys[i] = aeKeyToKey(completeAEKey)
	}
	return completeKeys, nil
}

func (dds *Ds) Delete(ctx context.Context, keys []ds.Key) error {
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

func (dds *Ds) AllocateKeys(ctx context.Context, key ds.Key, n int) (
	[]ds.Key, error) {

	var parentAEKey *datastore.Key
	if len(key.Path) > 1 {
		parentKey := ds.Key{
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

	keys := make([]ds.Key, n)
	for i := range keys {
		keys[i] = key
		keys[i].Path[childElemIndex].ID = low + int64(i)
	}
	return keys, nil
}

func (dds *Ds) Run(ctx context.Context, q ds.Query) (
	ds.Iterator, error) {
	keyPath := q.Root.Path
	aeQ := datastore.NewQuery(keyPath[len(keyPath)-1].Kind)

	if len(keyPath) > 1 {
		ancestorKey := ds.Key{
			Namespace: q.Root.Namespace,
			Path:      q.Root.Path[:len(q.Root.Path)-1],
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

	ctx, err := appengine.Namespace(ctx, q.Root.Namespace)
	if err != nil {
		return nil, err
	}

	return &datastoreIterator{
		iter: aeQ.Run(ctx),
	}, nil
}

type datastoreIterator struct {
	iter *datastore.Iterator
}

func (di *datastoreIterator) Next(entity interface{}) (ds.Key, error) {
	aeKey, err := di.iter.Next(entity)
	if err == datastore.Done {
		return ds.Key{}, nil
	} else if err != nil {
		return ds.Key{}, err
	}
	return aeKeyToKey(aeKey), nil
}

func (dds *Ds) RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	return dds.RunInTransactionFunc(ctx, f,
		&datastore.TransactionOptions{
			XG: true,
		})
}
