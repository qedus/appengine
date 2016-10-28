package ds_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/qedus/appengine/ds"
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

func TestAddRemoveDs(t *testing.T) {
	ctx, closeFunc, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer closeFunc()

	cDs := &callCountDs{}
	ctx = ds.AddDs(ctx, cDs)
	ds.Get(ctx, nil, nil)

	ds.RemoveDs(ctx, cDs)
	ds.Get(ctx, nil, nil)

	if cDs.getCount != 1 {
		t.Fatal("expected only one call to get")
	}
}

func TestDefaultDs(t *testing.T) {
	ctx, closeFunc, err := aetest.NewContext()
	if err != nil {
		t.Fatal(err)
	}
	defer closeFunc()

	dds := ds.NewDs()
	ctx = ds.AddDs(ctx, dds)

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

	type testEntity struct {
		Value int
	}

	dds := ds.NewDs()
	ctx = ds.AddDs(ctx, dds)

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
