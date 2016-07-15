# appengine

[![Build Status](https://travis-ci.org/qedus/appengine.svg?branch=master)](https://travis-ci.org/qedus/appengine) [![Coverage Status](https://coveralls.io/repos/github/qedus/appengine/badge.svg?branch=master)](https://coveralls.io/github/qedus/appengine?branch=master) [![GoDoc](https://godoc.org/github.com/qedus/appengine/datastore?status.png)](https://godoc.org/github.com/qedus/appengine/datastore)

A reworked Go API for App Engine's datastore that allows plugging in of different backends for testing.

# Update 15th July 2016

After using the [`datastore.Datastore`](https://godoc.org/github.com/qedus/appengine/datastore#Datastore) interface for a few days I have come to the conclusion that this API *should not* be used. Passing around a `datastore.Datastore` object between functions is a pain as most code in practice also needs a `context.Context` object passed in as well. Retrofitting old code is also a pain because of this. In the end functions end up as follow:

```
func mutatorFunction(ctx context.Context, ds datastore.TransactionalDatastore, ...) error {
    // Use datastore.
}
```

Or saving the `datastore.Datastore` in a context:

```
func mutatorFunction(ctx context.Context, ...) error {
    ds, ok := datastore.FromContext(ctx)
    if !ok {
        return errors.New("datastore not available")
    }
    
    // Use datastore.
}
```

Stay tuned for an updated API, with the same in memory database functionality but hopefully much easier to use in practice.
