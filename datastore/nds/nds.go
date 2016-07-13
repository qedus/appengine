package nds

import (
	"github.com/qedus/appengine/datastore"
	ids "github.com/qedus/appengine/internal/datastore"
	aends "github.com/qedus/nds"
	"golang.org/x/net/context"
	aeds "google.golang.org/appengine/datastore"
)

func New(ctx context.Context) datastore.TransactionalDatastore {
	cfg := ids.Config{
		Get:    aends.GetMulti,
		Put:    aends.PutMulti,
		Delete: aends.DeleteMulti,
		RunInTransaction: func(ctx context.Context,
			f func(context.Context) error) error {
			return aends.RunInTransaction(ctx,
				f, &aeds.TransactionOptions{XG: true})
		},
	}

	return ids.New(ctx, cfg)
}
