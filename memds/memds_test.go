package memds_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"

	"github.com/juju/testing/checkers"
	"github.com/qedus/appengine/datastore"
	"github.com/qedus/appengine/memds"
	"github.com/qedus/ds"
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

// compareDs allows us to call the datastore with multiple different
// implementations and compare the results.
type compareDs []ds.Ds

func (cds *compareDs) AllocateKeys(ctx context.Context, parent ds.Key, n int) (
	[]ds.Key, error) {

	compKeys := make([][]ds.Key, len(*cds))
	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compKeys[i], compErrs[i] = ds.AllocateKeys(ctx, parent, n)
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}

		if equal, err := checkers.DeepEqual(ce, compErrs[i+1]); !equal {
			return nil, fmt.Errorf("get errors not equal %v", err)
		}
	}

	for i, ck := range compKeys {
		if i >= len(compKeys)-1 {
			break
		}
		if len(ck) != len(compKeys[i+1]) {
			return nil, errors.New("key length not the same")
		}
	}

	return compKeys[0], nil
}

func (cds *compareDs) Get(ctx context.Context,
	keys []ds.Key, entities interface{}) error {

	ty := reflect.TypeOf(entities)

	compEntities := make([]interface{}, len(*cds))
	for i := range compEntities {
		// Assume the entities are slices which they should be.
		compEntities[i] = reflect.MakeSlice(ty,
			len(keys), len(keys)).Interface()
	}
	compEntities[0] = entities

	// Fill nil entities if required.
	if ty.Elem().Kind() == reflect.Ptr {

		values := reflect.ValueOf(entities)
		for i := range keys {
			val := values.Index(i)

			if !val.IsNil() {

				// For each of the datastores create a struct for its slice.
				for j := 1; j < len(*cds); j++ {
					compValues := reflect.ValueOf(compEntities[j])
					compValues.Index(i).Set(reflect.New(val.Type().Elem()))
				}
			}
		}
	}

	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compErrs[i] = ds.Get(ctx, keys, compEntities[i])
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if notFoundErr, ok := ce.(interface {
			NotFound(int) bool
		}); ok {

			// Check that the other error also implements NotFound.
			compNotFoundErr, ok := compErrs[i+1].(interface {
				NotFound(int) bool
			})
			if !ok {
				return fmt.Errorf("does not implement not found interface")
			}
			for i := range keys {
				if notFoundErr.NotFound(i) != compNotFoundErr.NotFound(i) {
					return fmt.Errorf("not found errors not equal")
				}
			}

		} else if equal, err := checkers.DeepEqual(ce, compErrs[i+1]); !equal {
			return fmt.Errorf("get errors not equal %v", err)
		}
	}

	// Check the returned entities are the same for each datastore if there are
	// no errors.
	for i, ce := range compEntities {
		if i >= len(compEntities)-1 {
			break
		}
		if equal, err := checkers.DeepEqual(ce, compEntities[i+1]); !equal {
			return fmt.Errorf("get entities not equal %v", err)
		}
	}

	// Returned errors are the same so just pick the first one to return.
	return compErrs[0]
}

func (cds *compareDs) Put(ctx context.Context,
	keys []ds.Key, entities interface{}) ([]ds.Key, error) {

	// Allocate IDs if any keys are incomplete.
	for i, key := range keys {
		keyIsIncomplete := key.Path[len(key.Path)-1].ID == nil
		if keyIsIncomplete {
			completeKeys, err := cds.AllocateKeys(ctx, key, 1)
			if err != nil {
				return nil, err
			}
			keys[i] = completeKeys[0]
		}
	}

	compKeys := make([][]ds.Key, len(*cds))
	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compKeys[i], compErrs[i] = ds.Put(ctx, keys, entities)
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compErrs[i+1]) {
			return nil, fmt.Errorf("put errors not equal %v vs %v",
				ce, compErrs[i+1])
		}
	}

	for i, ck := range compKeys {
		if i >= len(compKeys)-1 {
			break
		}

		// Don't worry about keys if there are errors.
		if compErrs[i] != nil {
			continue
		}
		for j, key := range ck {
			keyIsIncomplete := key.Path[len(key.Path)-1].ID == nil
			if keyIsIncomplete {
				// Don't worying about comparing keys if they were previously
				// incomplete as we can't predict which IDs will be used by the
				// varous datastores that might be used.
				continue
			}
			if !key.Equal(compKeys[i+1][j]) {
				return nil, errors.New("put keys not equal")
			}
		}
	}

	// Returned errors are the same so just pick the first one to return.
	return compKeys[0], compErrs[0]
}

func (cds *compareDs) Delete(ctx context.Context, keys []ds.Key) error {

	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compErrs[i] = ds.Delete(ctx, keys)
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compErrs[i+1]) {
			return errors.New("delete errors not equal")
		}
	}

	// Returned errors are the same so just pick the first one to return.
	return compErrs[0]
}

type compIterator []ds.Iterator

func (ci *compIterator) Next(entity interface{}) (ds.Key, error) {

	compEntities := make([]interface{}, len(*ci))

	if entity != nil {
		ty := reflect.TypeOf(entity)
		for i := range compEntities {
			// Assume the entities are slices which they should be.
			compEntities[i] = reflect.New(ty.Elem()).Interface()
		}
		compEntities[0] = entity
	}

	compKeys := make([]ds.Key, len(*ci))
	compErrs := make([]error, len(*ci))
	for i, iter := range *ci {
		compKeys[i], compErrs[i] = iter.Next(compEntities[i])
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compErrs[i+1]) {
			return ds.Key{}, fmt.Errorf("iter errors not the same %+v vs %+v",
				ce, compErrs[i+1])
		}
	}

	zeroKey := ds.Key{}

	// Check the returned keys are the same for each datastore.
	for i, ck := range compKeys {
		if i >= len(compKeys)-1 {
			break
		}
		if ck.Equal(zeroKey) && compKeys[i+1].Equal(zeroKey) {
			continue
		} else if ck.Equal(zeroKey) || compKeys[i+1].Equal(zeroKey) {
			return ds.Key{}, fmt.Errorf("iter keys not equal %+v vs %+v",
				ck, compKeys[i+1])
		}

		if !ck.Equal(compKeys[i+1]) {
			return ds.Key{}, fmt.Errorf("iter keys not equal %+v vs %+v",
				ck, compKeys[i+1])
		}
	}

	// Check the returned entities are the same for each datastore.
	for i, ce := range compEntities {
		if i >= len(compEntities)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compEntities[i+1]) {
			return zeroKey, fmt.Errorf("iter entities not equal %+v vs %+v",
				ce, compEntities[i+1])
		}
	}

	// Returned keys and errors are the same so pick the first one to return.
	return compKeys[0], compErrs[0]
}

func (cds *compareDs) Run(ctx context.Context, q ds.Query) (
	ds.Iterator, error) {

	iters := make(compIterator, len(*cds))
	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		iters[i], compErrs[i] = ds.Run(ctx, q)
	}

	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if equal, err := checkers.DeepEqual(ce, compErrs[i+1]); !equal {
			return nil, err
		}
	}

	return &iters, compErrs[0]
}

func (cds *compareDs) RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {

	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compErrs[i] = ds.RunInTransaction(ctx, f)
	}

	//  Check the returned errors are the same for each datastore.
	for i, ce := range compErrs {
		if i >= len(compErrs)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compErrs[i+1]) {
			return errors.New("tx errors not the same")
		}
	}
	return compErrs[0]
}

func TestPutGetDelete(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		memds.New(),
		datastore.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		Value int64
	}

	const kind = "Test"
	key := ds.NewKey("").Append(kind, "hi")

	putEntity := &testEntity{22}
	if _, err := ds.Put(ctx, []ds.Key{key},
		[]*testEntity{putEntity}); err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(ctx, []ds.Key{key},
		[]*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if putEntity.Value != getEntity.Value {
		t.Fatalf("entities not equivalent %+v vs %+v", putEntity, getEntity)
	}

	if err := ds.Delete(ctx, []ds.Key{key}); err != nil {
		t.Fatal(err)
	}

	if err := ds.Get(ctx, []ds.Key{key},
		[]*testEntity{&testEntity{}}); !isNotFoundErr(err, 0) {
		t.Fatal("expected to have deleted entity:", err)
	}

	// Check index values have been deleted.
	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("").Append(kind, nil),
	})
	if err != nil {
		t.Fatal(err)
	}

	if key, err := iter.Next(&testEntity{}); err != nil {
		t.Fatal(err)
	} else if key.Equal(ds.Key{}) {
		t.Fatal("expected no key")
	}
}

func TestTx(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		memds.New(),
		datastore.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		Value int64
	}

	key := ds.NewKey("").Append("Test", "up")

	if _, err := ds.Put(ctx, []ds.Key{key},
		[]*testEntity{&testEntity{3}}); err != nil {
		t.Fatal(err)
	}

	// Check delete doesn't work as we are returning an error.
	expectedErr := errors.New("expected error")
	if err := ds.RunInTransaction(ctx, func(ctx context.Context) error {
		if err := ds.Delete(ctx, []ds.Key{key}); err != nil {
			t.Fatal(err)
		}
		return expectedErr
	}); err != expectedErr {
		t.Fatal("expected", expectedErr, "got", err)
	}
	if err := ds.Get(ctx, []ds.Key{key},
		[]*testEntity{&testEntity{}}); err != nil {
		t.Fatal("expected an entity", err)
	}

	// Check delete does work now.
	if err := ds.RunInTransaction(ctx, func(ctx context.Context) error {
		return ds.Delete(ctx, []ds.Key{key})
	}); err != nil {
		t.Fatal(err)
	}

	if err := ds.Get(ctx, []ds.Key{key},
		[]*testEntity{&testEntity{}}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestQueryEqualFilter(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		Value int64
	}

	for i := 0; i < 10; i++ {
		key := ds.NewKey("").Append("Test", strconv.Itoa(i))
		entity := &testEntity{
			Value: int64(i),
		}
		if _, err := ds.Put(ctx, []ds.Key{key},
			[]*testEntity{entity}); err != nil {
			t.Fatal(err)
		}
	}

	q := ds.Query{
		Root: ds.NewKey("").Append("Test", nil),
		Orders: []ds.Order{
			{"Value", ds.AscDir},
		},
		Filters: []ds.Filter{
			{"Value", ds.EqualOp, int64(3)},
		},
	}

	iter, err := ds.Run(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	queryEntity := &testEntity{}
	key, err := iter.Next(queryEntity)
	if err != nil {
		t.Fatal(err)
	}
	if key.Equal(ds.Key{}) {
		t.Fatal("expected key")
	}
	if queryEntity.Value != 3 {
		t.Fatal("incorrect returned entity", key, queryEntity)
	}

	// Expect no entity.
	key, err = iter.Next(&testEntity{})
	if err != nil {
		t.Fatal(err)
	}
	if key.Equal(ds.Key{}) {
		t.Fatal("expected no key", key)
	}
}

func TestQueryOrder(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		memds.New(),
		datastore.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		Value int64
	}

	for i := 0; i < 10; i++ {
		key := ds.NewKey("").Append("Test", strconv.Itoa(i))
		if _, err := ds.Put(ctx, []ds.Key{key},
			[]*testEntity{&testEntity{int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	q := ds.Query{
		Root: ds.NewKey("").Append("Test", nil),
		Orders: []ds.Order{
			{"Value", ds.DescDir},
		},
	}

	iter, err := ds.Run(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		te := &testEntity{}
		key, err := iter.Next(te)
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			t.Fatal("expected key")
		}
		if te.Value != int64(9-i) {
			t.Fatal("incorrect returned entity", te)
		}
	}

	te := &testEntity{}
	key, err := iter.Next(te)
	if err != nil {
		t.Fatal(err)
	}
	if !key.Equal(ds.Key{}) {
		t.Fatal("expected no key", key)
	}
}

func TestAllocateKeys(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ctx = ds.NewContext(ctx, memds.New())

	parent := ds.NewKey("ns").Append("Parent", 2).Append("Test", nil)

	keys, err := ds.AllocateKeys(ctx, parent, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 10 {
		t.Fatal("incorrect returned keys")
	}

	for _, k := range keys {
		if k.Path[len(k.Path)-1].ID == nil {
			t.Fatal("incomplete key")
		}

		k.Path[len(k.Path)-1].ID = nil

		if !k.Equal(parent) {
			t.Fatal("incomplete key not same as parent")
		}
	}
}

func TestComplexValueSortOrder(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testString struct {
		Value string
	}

	type testInt struct {
		Value int64
	}

	type testFloat struct {
		Value float64
	}

	type testKey struct {
		Value ds.Key
	}

	type testTime struct {
		Value time.Time
	}

	type testBool struct {
		Value bool
	}

	keys := []ds.Key{
		ds.NewKey("").Append("Entity", "string"),
		ds.NewKey("").Append("Entity", "int"),
		ds.NewKey("").Append("Entity", "flaot"),
		ds.NewKey("").Append("Entity", "key"),
		ds.NewKey("").Append("Entity", "time"),
		ds.NewKey("").Append("Entity", "bool-true"),
		ds.NewKey("").Append("Entity", "bool-false"),
	}
	entities := []interface{}{
		&testString{"value"},
		&testInt{23},
		&testFloat{23.2},
		&testKey{ds.NewKey("").Append("KeyValue", "k")},
		&testTime{time.Unix(123456, 123456)},
		&testBool{true},
		&testBool{false},
	}

	if _, err := ds.Put(ctx, keys, entities); err != nil {
		t.Fatal(err)
	}

	iter, err := ds.Run(ctx, ds.Query{
		Root:     ds.NewKey("").Append("Entity", nil),
		KeysOnly: true,
		Orders: []ds.Order{
			{"Value", ds.AscDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i <= len(keys); i++ {
		_, err := iter.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	iter, err = ds.Run(ctx, ds.Query{
		Root:     ds.NewKey("").Append("Entity", nil),
		KeysOnly: true,
		Orders: []ds.Order{
			{"Value", ds.DescDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i <= len(keys); i++ {
		_, err := iter.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestKeyField(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		IntValue int64
		KeyValue ds.Key
	}

	key := ds.NewKey("ns").Append("Test", 2)
	keyValue := ds.NewKey("ns").Append("Value", "three")
	putEntity := &testEntity{
		IntValue: 5,
		KeyValue: keyValue,
	}

	keys, err := ds.Put(ctx, []ds.Key{key}, []*testEntity{putEntity})
	if err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(ctx, keys, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}
	if getEntity.IntValue != putEntity.IntValue {
		t.Fatal("int values not equal")
	}
	if !getEntity.KeyValue.Equal(putEntity.KeyValue) {
		t.Fatal("key values not equal")
	}

	// Now query for the key.
	q := ds.Query{
		Root: ds.NewKey("ns").Append("Test", nil),
		Filters: []ds.Filter{
			{"KeyValue", ds.EqualOp, keyValue},
		},
	}
	iter, err := ds.Run(ctx, q)
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

func TestKeyOrder(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		KeyValue ds.Key
	}

	keys := []ds.Key{
		// Check the difference between string and integer IDs.
		ds.NewKey("a").Append("Test", 2),
		ds.NewKey("a").Append("Test", "2"),

		// Check namespaces are isolated.
		ds.NewKey("").Append("Test", 2),
		ds.NewKey("b").Append("Test", 2),

		// Check key heirachy is ordered correctly.
		ds.NewKey("a").Append("Parent", 1).Append("Test", 2),
		ds.NewKey("a").Append("Parent", 2).Append("Test", 2),
	}

	// Make the entity keys the same as the key values to simplify testing both
	// order by key and order by property value.
	entities := make([]*testEntity, len(keys))
	for i, key := range keys {
		entities[i] = &testEntity{
			KeyValue: key,
		}
	}

	keys, err := ds.Put(ctx, keys, entities)
	if err != nil {
		t.Fatal(err)
	}

	// Ascending by key value.
	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("a").Append("Test", nil),
		Orders: []ds.Order{
			{"KeyValue", ds.AscDir},
		},
	})

	// The compareDs implementation of ds.Ds will do all the hard work of
	// ensuring we get the right entities compared to the App Engine datastore.
	for {
		key, err := iter.Next(&testEntity{})
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			break
		}
	}

	// Descending by key value.
	iter, err = ds.Run(ctx, ds.Query{
		Root: ds.NewKey("a").Append("Test", nil),
		Orders: []ds.Order{
			{"KeyValue", ds.DescDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The compareDs implementation of ds.Ds will do all the hard work of
	// ensuring we get the right entities compared to the App Engine datastore.
	for {
		key, err := iter.Next(&testEntity{})
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			break
		}
	}

	// Now check the same thing works with the actual entity keys.

	// Ascending by key.
	iter, err = ds.Run(ctx, ds.Query{
		Root: ds.NewKey("a").Append("Test", nil),
		Orders: []ds.Order{
			{"__key__", ds.AscDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The compareDs implementation of ds.Ds will do all the hard work of
	// ensuring we get the right entities compared to the App Engine datastore.
	for {
		key, err := iter.Next(&testEntity{})
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			break
		}
	}

	// Descending by key value.
	iter, err = ds.Run(ctx, ds.Query{
		Root: ds.NewKey("a").Append("Test", nil),
		Orders: []ds.Order{
			{"__key__", ds.DescDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The compareDs implementation of ds.Ds will do all the hard work of
	// ensuring we get the right entities compared to the App Engine datastore.
	for {
		key, err := iter.Next(&testEntity{})
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			break
		}
	}

}

func TestIntIDKeyOrder(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	keys := make([]ds.Key, 10)
	for i := range keys {
		keys[i] = ds.NewKey("test").Append("Test", int64(i+1))
	}
	entities := make([]struct{}, len(keys))

	if _, err := ds.Put(ctx, keys, entities); err != nil {
		t.Fatal(err)
	}

	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("test").Append("Test", nil),
		Orders: []ds.Order{
			{"__key__", ds.DescDir},
		},
		KeysOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	key, err := iter.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	if key.Path[len(key.Path)-1].ID.(int64) != 10 {
		t.Fatal("expected 10 got", key.Path[len(key.Path)-1].ID)
	}
}

func TestStructTags(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		ExcludeValue int64  `datastore:"-"`
		RenameValue  string `datastore:"newname"`
	}

	key := ds.NewKey("ns").Append("Kind", nil)
	putEntity := &testEntity{
		ExcludeValue: 20,
		RenameValue:  "hi there",
	}

	keys, err := ds.Put(ctx, []ds.Key{key}, []*testEntity{putEntity})
	if err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(ctx, keys, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if getEntity.ExcludeValue != 0 {
		t.Fatal("expected value to be excluded")
	}
	if getEntity.RenameValue != "hi there" {
		t.Fatal("incorrect rename value")
	}

	// Query for the renamed value.
	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("ns").Append("Kind", nil),
		Filters: []ds.Filter{
			{"newname", ds.EqualOp, "hi there"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	queryEntity := &testEntity{}
	key, err = iter.Next(queryEntity)
	if err != nil {
		t.Fatal(err)
	}
	if key.Equal(ds.Key{}) {
		t.Fatal("key is nil")
	}
	if !key.Equal(keys[0]) {
		t.Fatal("incorrect key")
	}

	if queryEntity.ExcludeValue != 0 {
		t.Fatal("expected value to be excluded")
	}
	if queryEntity.RenameValue != "hi there" {
		t.Fatal("incorrect rename value")
	}
}

func TestSliceProperties(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		IntValues []int64
	}

	key := ds.NewKey("").Append("Kind", 3)
	intValues := []int64{1, 2, 3, 4}
	putEntity := &testEntity{
		IntValues: intValues,
	}

	keys, err := ds.Put(ctx, []ds.Key{key}, []*testEntity{putEntity})
	if err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(ctx, keys, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(getEntity.IntValues, intValues) {
		t.Fatal("incorrect int64 values", getEntity.IntValues)
	}

	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("").Append("Kind", nil),
		Filters: []ds.Filter{
			{"IntValues", ds.GreaterThanOp, int64(2)},
		},
		Orders: []ds.Order{
			{"IntValues", ds.AscDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for {
		entity := &testEntity{}
		key, err := iter.Next(entity)
		if err != nil {
			t.Fatal(err)
		}
		if key.Equal(ds.Key{}) {
			break
		}
	}
}

func TestAncestorQuery(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	type testEntity struct {
		IntValue int64
	}

	keys := []ds.Key{
		ds.NewKey("").Append("Parent", 1).Append("Child", 1),
		ds.NewKey("").Append("Parent", 2).Append("Child", 2),
		ds.NewKey("").Append("Parent", 1).Append("Child", 3),
		ds.NewKey("").Append("Child", 4),
	}
	putEntities := []*testEntity{{1}, {2}, {3}, {4}}

	if _, err := ds.Put(ctx, keys, putEntities); err != nil {
		t.Fatal(err)
	}

	iter, err := ds.Run(ctx, ds.Query{
		Root: ds.NewKey("").Append("Parent", 1).Append("Child", nil),
		Orders: []ds.Order{
			{"__key__", ds.DescDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	iterEntity := &testEntity{}
	if key, err := iter.Next(iterEntity); err != nil {
		t.Fatal(err)
	} else if key.Equal(ds.Key{}) {
		t.Fatal("no data")
	} else if !key.Equal(keys[2]) {
		t.Fatal("keys not equal")

	} else if iterEntity.IntValue != 3 {
		t.Fatal("incorrect int value")
	}

	iterEntity = &testEntity{}
	if key, err := iter.Next(iterEntity); err != nil {
		t.Fatal(err)
	} else if key.Equal(ds.Key{}) {
		t.Fatal("no data")
	} else if !key.Equal(keys[0]) {
		t.Fatal("keys not equal")
	} else if iterEntity.IntValue != 1 {
		t.Fatal("incorrect int value")
	}

	if key, err := iter.Next(&testEntity{}); err != nil {
		t.Fatal(err)
	} else if !key.Equal(ds.Key{}) {
		t.Fatal("expected nil key")
	}
}

func TestByteSliceProperties(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	cds := &compareDs{
		datastore.New(),
		memds.New(),
	}

	ctx = ds.NewContext(ctx, cds)

	// []byte should be treated as a single property like string, not a slice.
	type testEntity struct {
		ByteValue []byte `datastore:",noindex"`
	}

	key := ds.NewKey("").Append("Kind", 3)
	byteValue := []byte("hi there")
	putEntity := &testEntity{
		ByteValue: byteValue,
	}

	keys, err := ds.Put(ctx, []ds.Key{key}, []*testEntity{putEntity})
	if err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get(ctx, keys, []*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(getEntity.ByteValue, byteValue) {
		t.Fatal("incorrect byte values", getEntity.ByteValue)
	}
}
