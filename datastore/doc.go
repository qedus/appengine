/*
Package datastore is a datastore API for Google App Engine. Unlike
https://google.golang.org/appengine/datastore functionality is provided through
interfaces to allow easy use of different backends. There are currently three
backends implemented:

1) https://godoc.org/github.com/qedus/appengine/datastore/ds which provides an
implementation that maps directly to the datastore. It will connect to the
production datastore or dev_appserver.py datastore in development and can be
instantiated as follows:

	import (
		"net/http"

		"github.com/qedus/appengine/datastore/ds"
		"google.golang.org/appengine"
	)

	func handleEvent(w http.ResponseWriter, r *http.Reader) {
		ctx := appengine.NewContext(r)
		ds := ds.New(ctx)
		mutateDatastore(ds)
		...
	}

2) https://godoc.org/github.com/qedus/appengine/datastore/nds which provides an
implementation that maps directly to https://godoc.org/github.com/qedus/nds and
and gives strong consistency memcaching of datastore calls. It will connect to
the production datastore or dev_appserver.py datastore in development and can be
instantiated as follows:

	import (
		"net/http"

		"github.com/qedus/appengine/datastore/nds"
		"google.golang.org/appengine"
	)

	func handleEvent(w http.ResponseWriter, r *http.Reader) {
		ctx := appengine.NewContext(r)
		ds := nds.New(ctx)
		mutateDatastore(ds)
		...
	}

3) https://godoc.org/github.com/qedus/appengine/datastore/memds which provides a
pure memory based implementation of the datastore and is used for fast testing
without the need for aetest. It can be used as follows.

	import (
		"testing"

		"github.com/qedus/appengine/datastore/memds"
	)

	func TestMutateDatastore(t *testing.T) {

		ds := memds.New()
		if err := mutateDatastore(ds); err != nil {
			t.Fatal(err)
		}
	}

Motivation

I got fed up using dev_appserver.py and google.golang.org/appengine/aetest to
test my code. I kept getting slow execution, timeouts, unpredictable crashes and
zombie/orphan processes. For larger integration testing I found that I needed
to include time.Sleep() throughout my tests to reduce the chances of the
dev_appserver.py crashing mid test. Therefore I thought I would have a go at
making my own entirly Go based dev_appserver.py that would execute quickly and
predictably.

Status

A PropertyLoadSaver equivalent is not implemented.
*/
package datastore
