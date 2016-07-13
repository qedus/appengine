package ds

import (
	"github.com/qedus/appengine/datastore"
	ids "github.com/qedus/appengine/internal/datastore"
	"golang.org/x/net/context"
	aeds "google.golang.org/appengine/datastore"
)

func New(ctx context.Context) datastore.TransactionalDatastore {
	cfg := ids.Config{
		Get:    aeds.GetMulti,
		Put:    aeds.PutMulti,
		Delete: aeds.DeleteMulti,
		RunInTransaction: func(ctx context.Context,
			f func(context.Context) error) error {
			return aeds.RunInTransaction(ctx,
				f, &aeds.TransactionOptions{XG: true})
		},
	}

	return ids.New(ctx, cfg)
}
