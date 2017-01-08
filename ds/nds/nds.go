package nds

import (
	"github.com/qedus/appengine/ds"
	"github.com/qedus/appengine/ds/datastore"
	"github.com/qedus/nds"
)

func New() ds.Ds {
	return &datastore.Ds{
		GetFunc:              nds.GetMulti,
		PutFunc:              nds.PutMulti,
		DeleteFunc:           nds.DeleteMulti,
		RunInTransactionFunc: nds.RunInTransaction,
	}
}
