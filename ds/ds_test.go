package ds_test

import (
	"fmt"
	"testing"

	"github.com/qedus/appengine/ds"
	"google.golang.org/appengine/aetest"
)

func TestKey(t *testing.T) {

	key := ds.Key{
		Namespace: "ns",
		Kind:      "kind",
		IDs: []ds.ID{
			ds.StringID("here"),
			ds.IntID(2234),
		},
	}
	fmt.Println(key)
}

type callCountDs struct {
	ds.Ds

	getCount int
}

func (ds *callCountDs) Get(entities []ds.Entity) error {
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
	ds.Get(ctx, nil)

	ds.RemoveDs(ctx, cDs)
	ds.Get(ctx, nil)

	if cDs.getCount != 1 {
		t.Fatal("expected only one call to get")
	}
}
