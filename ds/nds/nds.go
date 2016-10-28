package nds

import (
	"github.com/qedus/appengine/ds"
	aends "github.com/qedus/nds"
)

func New() ds.Ds {
	return &ds.DefaultDs{
		GetFunc:              aends.GetMulti,
		PutFunc:              aends.PutMulti,
		DeleteFunc:           aends.DeleteMulti,
		RunInTransactionFunc: aends.RunInTransaction,
	}
}
