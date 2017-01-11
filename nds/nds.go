package nds

import (
	"github.com/qedus/appengine/datastore"
	"github.com/qedus/ds"
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
