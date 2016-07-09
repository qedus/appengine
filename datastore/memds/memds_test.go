package memds_test

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"

	"github.com/juju/testing/checkers"
	"github.com/qedus/appengine/datastore"
	"github.com/qedus/appengine/datastore/memds"
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
type compareDs []datastore.TransactionalDatastore

func (cds *compareDs) Get(keys []datastore.Key, entities interface{}) error {

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
		compErrs[i] = ds.Get(keys, compEntities[i])
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

func (cds *compareDs) Put(keys []datastore.Key, entities interface{}) (
	[]datastore.Key, error) {

	compKeys := make([][]datastore.Key, len(*cds))
	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compKeys[i], compErrs[i] = ds.Put(keys, entities)
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
			if !key.Equal(compKeys[i+1][j]) {
				return nil, errors.New("put keys not equal")
			}
		}
	}

	// Returned errors are the same so just pick the first one to return.
	return keys, compErrs[0]
}

func (cds *compareDs) Delete(keys []datastore.Key) error {

	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compErrs[i] = ds.Delete(keys)
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

type compIterator []datastore.Iterator

func (ci *compIterator) Next(entity interface{}) (datastore.Key, error) {

	compEntities := make([]interface{}, len(*ci))

	if entity != nil {
		ty := reflect.TypeOf(entity)
		for i := range compEntities {
			// Assume the entities are slices which they should be.
			compEntities[i] = reflect.New(ty.Elem()).Interface()
		}
		compEntities[0] = entity
	}

	compKeys := make([]datastore.Key, len(*ci))
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
			return nil, fmt.Errorf("iter errors not the same %+v vs %+v",
				ce, compErrs[i+1])
		}
	}

	// Check the returned keys are the same for each datastore.
	for i, ck := range compKeys {
		if i >= len(compKeys)-1 {
			break
		}
		if ck == nil && compKeys[i+1] == nil {
			continue
		} else if ck == nil || compKeys[i+1] == nil {
			return nil, fmt.Errorf("iter keys not equal %+v vs %+v",
				ck, compKeys[i+1])
		}

		if !ck.Equal(compKeys[i+1]) {
			return nil, fmt.Errorf("iter keys not equal %+v vs %+v",
				ck, compKeys[i+1])
		}
	}

	// Check the returned entities are the same for each datastore.
	for i, ce := range compEntities {
		if i >= len(compEntities)-1 {
			break
		}
		if !reflect.DeepEqual(ce, compEntities[i+1]) {
			return nil, fmt.Errorf("iter entities not equal %+v vs %+v",
				ce, compEntities[i+1])
		}
	}

	// Returned keys and errors are the same so pick the first one to return.
	return compKeys[0], compErrs[0]
}

func (cds *compareDs) Run(q datastore.Query) (datastore.Iterator, error) {

	iters := make(compIterator, len(*cds))
	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		iters[i], compErrs[i] = ds.Run(q)
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

func (cds *compareDs) RunInTransaction(f func(ds datastore.Datastore) error) error {

	compErrs := make([]error, len(*cds))
	for i, ds := range *cds {
		compErrs[i] = ds.RunInTransaction(f)
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

	ds := &compareDs{
		memds.New(),
		datastore.New(ctx),
	}

	type testEntity struct {
		Value int64
	}

	const kind = "Test"
	key := datastore.NewKey("").StringID(kind, "hi")

	putEntity := &testEntity{22}
	if _, err := ds.Put([]datastore.Key{key},
		[]*testEntity{putEntity}); err != nil {
		t.Fatal(err)
	}

	getEntity := &testEntity{}
	if err := ds.Get([]datastore.Key{key},
		[]*testEntity{getEntity}); err != nil {
		t.Fatal(err)
	}

	if putEntity.Value != getEntity.Value {
		t.Fatalf("entities not equivalent %+v vs %+v", putEntity, getEntity)
	}

	if err := ds.Delete([]datastore.Key{key}); err != nil {
		t.Fatal(err)
	}

	if err := ds.Get([]datastore.Key{key},
		[]*testEntity{&testEntity{}}); !isNotFoundErr(err, 0) {
		t.Fatal("expected to have deleted entity:", err)
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

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		memds.New(),
		datastore.New(ctx),
	}

	type testEntity struct {
		Value int64
	}

	key := datastore.NewKey("").StringID("Test", "up")

	if _, err := ds.Put([]datastore.Key{key},
		[]*testEntity{&testEntity{3}}); err != nil {
		t.Fatal(err)
	}

	// Check delete doesn't work as we are returning an error.
	expectedErr := errors.New("expected error")
	if err := ds.RunInTransaction(func(txDs datastore.Datastore) error {
		if err := txDs.Delete([]datastore.Key{key}); err != nil {
			t.Fatal(err)
		}
		return expectedErr
	}); err != expectedErr {
		t.Fatal("expected", expectedErr, "got", err)
	}
	if err := ds.Get([]datastore.Key{key},
		[]*testEntity{&testEntity{}}); err != nil {
		t.Fatal("expected an entity", err)
	}

	// Check delete does work now.
	if err := ds.RunInTransaction(func(txDs datastore.Datastore) error {
		return txDs.Delete([]datastore.Key{key})
	}); err != nil {
		t.Fatal(err)
	}

	if err := ds.Get([]datastore.Key{key},
		[]*testEntity{&testEntity{}}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestQueryEqualFilter(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		datastore.New(ctx),
		memds.New(),
	}

	type testEntity struct {
		Value int64
	}

	for i := 0; i < 10; i++ {
		key := datastore.NewKey("").StringID("Test", strconv.Itoa(i))
		entity := &testEntity{
			Value: int64(i),
		}
		if _, err := ds.Put([]datastore.Key{key},
			[]*testEntity{entity}); err != nil {
			t.Fatal(err)
		}
	}

	q := datastore.Query{
		Kind: "Test",
		Orders: []datastore.Order{
			{"Value", datastore.AscDir},
		},
		Filters: []datastore.Filter{
			{"Value", int64(3), datastore.EqualOp},
		},
	}

	iter, err := ds.Run(q)
	if err != nil {
		t.Fatal(err)
	}

	queryEntity := &testEntity{}
	key, err := iter.Next(queryEntity)
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
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
	if key != nil {
		t.Fatal("expected no key", key)
	}
}

func TestQueryOrder(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		memds.New(),
		datastore.New(ctx),
	}

	type testEntity struct {
		Value int64
	}

	for i := 0; i < 10; i++ {
		key := datastore.NewKey("").StringID("Test", strconv.Itoa(i))
		if _, err := ds.Put([]datastore.Key{key},
			[]*testEntity{&testEntity{int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	q := datastore.Query{
		Kind: "Test",
		Orders: []datastore.Order{
			{"Value", datastore.DescDir},
		},
	}

	iter, err := ds.Run(q)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		te := &testEntity{}
		key, err := iter.Next(te)
		if err != nil {
			t.Fatal(err)
		}
		if key == nil {
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
	if key != nil {
		t.Fatal("expected no key", key)
	}
}

func TestAllocateKeys(t *testing.T) {

	ds := memds.New()

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

func TestComplexValueSortOrder(t *testing.T) {

	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		datastore.New(ctx),
		memds.New(),
	}

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
		Value datastore.Key
	}

	if _, err := ds.Put([]datastore.Key{
		datastore.NewKey("").StringID("Entity", "string"),
		datastore.NewKey("").StringID("Entity", "int"),
		datastore.NewKey("").StringID("Entity", "flaot"),
		datastore.NewKey("").StringID("Entity", "key"),
	},
		[]interface{}{
			&testString{"value"},
			&testInt{23},
			&testFloat{23.2},
			&testKey{datastore.NewKey("").StringID("KeyValue", "k")},
		}); err != nil {
		t.Fatal(err)
	}

	iter, err := ds.Run(datastore.Query{
		Kind:     "Entity",
		KeysOnly: true,
		Orders: []datastore.Order{
			{"Value", datastore.AscDir},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		_, err := iter.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestKeyField(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		datastore.New(ctx),
		memds.New(),
	}

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

func TestKeyOrder(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		datastore.New(ctx),
		memds.New(),
	}

	type testEntity struct {
		KeyValue datastore.Key
	}

	keys := []datastore.Key{
		// Check the difference between string and integer IDs.
		datastore.NewKey("a").IntID("Test", 2),
		datastore.NewKey("a").StringID("Test", "2"),

		// Check namespaces are isolated.
		datastore.NewKey("").IntID("Test", 2),
		datastore.NewKey("b").IntID("Test", 2),

		// Check key heirachy is ordered correctly.
		datastore.NewKey("a").IntID("Parent", 1).IntID("Test", 2),
		datastore.NewKey("a").IntID("Parent", 2).IntID("Test", 2),
	}

	// Make the entity keys the same as the key values to simplify testing both
	// order by key and order by property value.
	entities := make([]*testEntity, len(keys))
	for i, key := range keys {
		entities[i] = &testEntity{
			KeyValue: key,
		}
	}

	keys, err := ds.Put(keys, entities)
	if err != nil {
		t.Fatal(err)
	}

	// Ascending by key value.
	iter, err := ds.Run(datastore.Query{
		Namespace: "a",
		Kind:      "Test",
		Orders: []datastore.Order{
			{"KeyValue", datastore.AscDir},
		},
	})

	// The compareDs implementation of ds.Ds will do all the hard work of
	// ensuring we get the right entities compared to the App Engine datastore.
	for {
		key, err := iter.Next(&testEntity{})
		if err != nil {
			t.Fatal(err)
		}
		if key == nil {
			break
		}
	}

	// Descending by key value.
	iter, err = ds.Run(datastore.Query{
		Namespace: "a",
		Kind:      "Test",
		Orders: []datastore.Order{
			{"KeyValue", datastore.DescDir},
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
		if key == nil {
			break
		}
	}

	// Now check the same thing works with the actual entity keys.

	// Ascending by key.
	iter, err = ds.Run(datastore.Query{
		Namespace: "a",
		Kind:      "Test",
		Orders: []datastore.Order{
			{datastore.KeyName, datastore.AscDir},
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
		if key == nil {
			break
		}
	}

	// Descending by key value.
	iter, err = ds.Run(datastore.Query{
		Namespace: "a",
		Kind:      "Test",
		Orders: []datastore.Order{
			{datastore.KeyName, datastore.DescDir},
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
		if key == nil {
			break
		}
	}

}

func TestIntIDKeyOrder(t *testing.T) {
	ctx, closeFunc := newContext(t, true)
	defer closeFunc()

	ds := &compareDs{
		datastore.New(ctx),
		memds.New(),
	}

	keys := make([]datastore.Key, 10)
	for i := range keys {
		keys[i] = datastore.NewKey("test").IntID("Test", int64(i+1))
	}
	entities := make([]struct{}, len(keys))

	if _, err := ds.Put(keys, entities); err != nil {
		t.Fatal(err)
	}

	iter, err := ds.Run(datastore.Query{
		Namespace: "test",
		Kind:      "Test",
		Orders: []datastore.Order{
			{datastore.KeyName, datastore.DescDir},
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
	if key.ID().(int64) != 10 {
		t.Fatal("expected 10 got", key.ID())
	}
}
