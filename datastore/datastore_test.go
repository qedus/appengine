package datastore_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/qedus/appengine/datastore"
	"github.com/qedus/ds"
	"google.golang.org/appengine/aetest"
)

func TestKey(t *testing.T) {

	/*
		key := ds.Key{
			Namespace: "ns",
			Kind:      "kind",
			IDs: []ds.ID{
				ds.StringID("here"),
				ds.IntID(2234),
			},
		}
		fmt.Println(key)
	*/
}

type callCountDs struct {
	ds.Ds

	getCount int
}

func (ds *callCountDs) Get(ctx context.Context,
	keys []ds.Key, entities interface{}) error {
	ds.getCount++
	return nil
}

func TestDefaultDs(t *testing.T) {
	ctx, closeFunc, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer closeFunc()

	ctx = ds.NewContext(ctx, datastore.New())

	key := ds.NewKey("").Append("TestKind", nil)

	type testEntity struct {
		String string
	}
	keys, err := ds.Put(ctx, []ds.Key{key},
		[]*testEntity{&testEntity{"hi there"}})
	if err != nil {
		t.Fatal(err)
	}

	if keys[0].Path[len(keys[0].Path)-1].ID == nil {
		t.Fatal("expected ID to not be nil")
	}
}

func TestRunInTransaction(t *testing.T) {
	ctx, closeFunc, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer closeFunc()

	ctx = ds.NewContext(ctx, datastore.New())

	type testEntity struct {
		Value int
	}

	if err := ds.RunInTransaction(ctx, func(tctx context.Context) error {
		key := ds.NewKey("").Append("Test", 23)
		if _, err := ds.Put(tctx,
			[]ds.Key{key}, []*testEntity{&testEntity{42}}); err != nil {
			return err
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
