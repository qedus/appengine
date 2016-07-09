/*
Package datastore is a Go API for Google App Engine analagous to
google.golang.org/appengine/datastore. However functionality is provided through
interfaces that allow the easy use of backends other than the App Engine
datastore. You will find a memory based backend in memds directory to create
fast unit tests without needing dev_appserver.py..

Motivation

I got fed up using dev_appserver.py and google.golang.org/appengine/aetest to
test my code. I kept getting slow execution, timeouts, unpredictable crashes and
zombie/orphan processes. For larger integration testing I found that I needed
to include time.Sleep() throughout my tests to reduce the chances of the
dev_appserver.py crashing mid test. Therefore I thought I would have a go at
making my own entirly Go based dev_appserver.py that would execute quickly and
predictably.

Status

The code as it stands now is a proof of concept.
*/
package datastore
