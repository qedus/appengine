package datastore_test

import (
	"errors"
	"testing"

	"github.com/qedus/appengine/datastore"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
)

func isNotFoundErr(err error, index int) bool {
	nfe, ok := err.(interface {
		NotFound(int) bool
	})
	return ok && nfe.NotFound(index)
}

func newContext(t *testing.T, stronglyConsistentDatastore bool) (
	context.Context, func()) {
	inst, err := aetest.NewInstance(&aetest.Options{
		StronglyConsistentDatastore: stronglyConsistentDatastore,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		inst.Close()
		t.Fatal(err)
	}

	return appengine.NewContext(req), func() {
		inst.Close()
	}
}

func TestPutGetDelete(t *testing.T) {
	ctx, closeFunc := newContext(t, false)
	defer closeFunc()

	ds := datastore.New(ctx)

	type testEntity struct {
		Value int64
	}

	const kind = "Test"
	key := datastore.NewKey("").StringID(kind, "hi")

	putEntity := &testEntity{
		Value: 22,
	}
	if _, err := ds.Put([]datastore.Key{key},
		[]*testEntity{putEntity}); err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get([]datastore.Key{key}, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if putEntity.Value != getEntity.Value {
		t.Fatalf("entities not equivalent %+v vs %+v", putEntity, getEntity)
	}

	if err := ds.Delete([]datastore.Key{key}); err != nil {
		t.Fatal(err)
	}

	nfe, ok := ds.Get([]datastore.Key{key},
		[]*testEntity{&testEntity{}}).(interface {
		NotFound(int) bool
	})
	if !ok {
		t.Fatal("expected not found interface")
	}
	if !nfe.NotFound(0) {
		t.Fatal("expected to have deleted entity")
	}

	// Check index values have been deleted.
	iter, err := ds.Run(datastore.Query{
		Kind: kind,
	})
	if err != nil {
		t.Fatal(err)
	}

	if key, err := iter.Next(&testEntity{}); err != nil {
		t.Fatal(err)
	} else if key != nil {
		t.Fatal("expected no key")
	}
}

func TestTx(t *testing.T) {
	ctx, closeFunc := newContext(t, false)
	defer closeFunc()

	ds := datastore.New(ctx)

	type testEntity struct {
		Value int64
	}

	key := datastore.NewKey("").IntID("Test", 2)
	putEntity := &testEntity{
		Value: 2,
	}

	// Test normal transactions work.
	if err := ds.RunInTransaction(func(ds datastore.Datastore) error {
		if _, err := ds.Put([]datastore.Key{key},
			[]*testEntity{putEntity}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get([]datastore.Key{key}, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}
	if getEntity.Value != 2 {
		t.Fatal("incorrect value")
	}

	// Try to delete entity but raise error instead.
	expectedErr := errors.New("expected error")
	if err := ds.RunInTransaction(func(ds datastore.Datastore) error {
		if err := ds.Delete([]datastore.Key{key}); err != nil {
			return err
		}
		return expectedErr
	}); err != expectedErr {
		t.Fatal("expecting error")
	}

	// Should still be able to get the entity.
	getEntity = &testEntity{}
	if err := ds.Get([]datastore.Key{key}, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}
	if getEntity.Value != 2 {
		t.Fatal("incorrect value")
	}

	if err := ds.RunInTransaction(func(ds datastore.Datastore) error {
		return ds.Delete([]datastore.Key{key})
	}); err != nil {
		t.Fatal(err)
	}

	// Now should not be able to get the entity.
	if err := ds.Get([]datastore.Key{key},
		[]*testEntity{&testEntity{}}); !isNotFoundErr(err, 0) {
		t.Fatal("expected not found error", err)
	}
}

func TestAllocateKeys(t *testing.T) {
	ctx, closeFunc := newContext(t, false)
	defer closeFunc()

	ds := datastore.New(ctx)

	key := datastore.NewKey("ns").IntID("Parent", 2).IncompleteID("Test")

	keys, err := ds.AllocateKeys(key, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 10 {
		t.Fatal("incorrect returned keys")
	}

	for _, k := range keys {
		if k.Namespace() != key.Namespace() {
			t.Fatal("incorrect namespace")
		}
		if !k.Parent().Equal(key.Parent()) {
			t.Fatal("incorrect parents: wanted", key, "got", k)
		}
		if k.Kind() != key.Kind() {
			t.Fatal("incorrect kind")
		}
		if k.Incomplete() {
			t.Fatal("incomplete key")
		}
	}
}

func TestPutIncompleteKey(t *testing.T) {
	ctx, closeFunc := newContext(t, false)
	defer closeFunc()

	type testEntity struct {
		Value int64
	}

	ds := datastore.New(ctx)

	key := datastore.NewKey("ns").IncompleteID("Kind")
	keys, err := ds.Put([]datastore.Key{key}, []*testEntity{&testEntity{4}})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatal("incorrect key length")
	}
	key = keys[0]

	if key.Incomplete() {
		t.Fatal("incomplete key")
	}

	if key.Kind() != "Kind" {
		t.Fatal("incorrect kind")
	}

	if key.Namespace() != "ns" {
		t.Fatal("incorrect namespace")
	}
}

func TestKeyField(t *testing.T) {
	ctx, closeFunc := newContext(t, false)
	defer closeFunc()

	ds := datastore.New(ctx)

	type testEntity struct {
		IntValue int64
		KeyValue datastore.Key
	}

	key := datastore.NewKey("").IntID("Test", 2)
	keyValue := datastore.NewKey("ns").StringID("Value", "three")
	putEntity := &testEntity{
		IntValue: 5,
		KeyValue: keyValue,
	}

	keys, err := ds.Put([]datastore.Key{key}, []*testEntity{putEntity})
	if err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(keys, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}
	if getEntity.IntValue != putEntity.IntValue {
		t.Fatal("int values not equal")
	}
	if !getEntity.KeyValue.Equal(putEntity.KeyValue) {
		t.Fatal("key values not equal")
	}

	// Now query for the key.
	q := datastore.Query{
		Kind: "Test",
		Filters: []datastore.Filter{
			{"KeyValue", keyValue, datastore.EqualOp},
		},
	}
	iter, err := ds.Run(q)
	if err != nil {
		t.Fatal(err)
	}

	queryEntity := &testEntity{}
	queryKey, err := iter.Next(queryEntity)
	if err != nil {
		t.Fatal(err)
	}
	if !key.Equal(queryKey) {
		t.Fatal("incorrect key")
	}

	if queryEntity.IntValue != putEntity.IntValue {
		t.Fatal("incorrect int value")
	}

	if !queryEntity.KeyValue.Equal(keyValue) {
		t.Fatal("incorrect key value")
	}
}
