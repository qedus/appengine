package memds

import (
	"errors"
	"reflect"
	"sort"
	"strings"

	"github.com/qedus/appengine/datastore"
)

type notFoundError map[int]bool

func (nfe notFoundError) Error() string {
	return "entities not found"
}

func (nfe notFoundError) NotFound(index int) bool {
	return nfe[index]
}

type keyEntity struct {
	key    datastore.Key
	entity interface{}
}

type keyValue struct {
	key   datastore.Key
	value interface{}
}

type ds struct {
	keyEntities []*keyEntity
	lastIntID   int64
}

// New creates a new TransationalDatastore that resides solely in memory. It is
// useful for fast unit testing datastore code compared to using
// google.golang.org/appengine/aetest.
func New() datastore.TransactionalDatastore {
	return &ds{
		keyEntities: []*keyEntity{},
	}
}

func (ds *ds) nextIntID() int64 {
	ds.lastIntID++
	return ds.lastIntID
}

func extractStruct(entity interface{}) (reflect.Value, error) {
	// Only accept struct pointers.
	val := reflect.ValueOf(entity)
	if val.Kind() != reflect.Ptr {
		return reflect.Value{},
			errors.New("memds: entity must be a pointer to a struct")
	}

	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return reflect.Value{},
			errors.New("memds: entity must be a pointer to a struct")
	}
	return val, nil
}

func (ds *ds) Get(keys []datastore.Key, entities interface{}) error {
	values := reflect.ValueOf(entities)

	if err := verifyKeysValues(keys, values); err != nil {
		return err
	}

	nfe := notFoundError{}
	for i, key := range keys {
		value := values.Index(i)

		found, err := ds.get(key, value.Interface())
		if err != nil {
			return err
		}
		if !found {
			nfe[i] = true
		}
	}

	if len(nfe) == 0 {
		return nil
	}

	return nfe
}

func (ds *ds) get(key datastore.Key, entity interface{}) (bool, error) {

	val, err := extractStruct(entity)
	if err != nil {
		return false, err
	}

	ke := ds.findKeyEntity(key)
	if ke == nil {
		return false, nil
	}
	val.Set(reflect.ValueOf(ke.entity))

	return true, nil
}

func (ds *ds) findKeyEntity(key datastore.Key) *keyEntity {
	// TODO: Use sorted version.
	for _, ke := range ds.keyEntities {
		if ke.key.Equal(key) {
			return ke
		}
	}
	return nil
}

func verifyKeysValues(keys []datastore.Key, values reflect.Value) error {
	if values.Kind() != reflect.Slice {
		return errors.New("entities not a slice")
	}

	if len(keys) != values.Len() {
		return errors.New("keys length not same as entities length")
	}

	sliceEntityType := values.Type().Elem()
	switch sliceEntityType.Kind() {
	case reflect.Struct:
		return nil
	case reflect.Ptr:
		if sliceEntityType.Elem().Kind() == reflect.Struct {
			return nil
		}
	case reflect.Interface:
		// Need to check that each value is a struct pointer as per App Engine
		// requirements.
		for i := 0; i < values.Len(); i++ {
			val := values.Index(i)
			// Check the interface value is a pointer to a struct.
			if val.Kind() == reflect.Interface &&
				val.Elem().Kind() == reflect.Ptr &&
				val.Elem().Elem().Kind() == reflect.Struct {
				continue
			}

			// Not a pointer to a struct so raise an error.
			return errors.New(
				"interface slice does not contain struct pointers")
		}
		return nil
	}
	return errors.New("entities not structs or pointers")
}

func (ds *ds) Put(keys []datastore.Key, entities interface{}) ([]datastore.Key, error) {
	values := reflect.ValueOf(entities)

	if err := verifyKeysValues(keys, values); err != nil {
		return nil, err
	}

	completeKeys := make([]datastore.Key, len(keys))
	for i, key := range keys {
		val := values.Index(i)
		completeKey, err := ds.put(key, val.Interface())
		if err != nil {
			return nil, err
		}
		completeKeys[i] = completeKey
	}

	return completeKeys, nil
}

func (ds *ds) put(key datastore.Key, entity interface{}) (datastore.Key, error) {

	// If key is incomplete then complete it.
	if key.Incomplete() {
		namespace := key.Namespace()
		kind := key.Kind()

		parent := key.Parent()
		if parent == nil {
			key = datastore.NewKey(namespace).IntID(kind, ds.nextIntID())
		} else {
			key = parent.IntID(kind, ds.nextIntID())
		}
	}

	val := reflect.ValueOf(entity)
	switch val.Kind() {
	case reflect.Ptr:
		val = val.Elem()
		if val.Kind() != reflect.Struct {
			return nil, errors.New("memds: entity not struct or struct pointer")
		}
	case reflect.Struct:
		// Allowed entity kind.
	default:
		return nil, errors.New("memds: entity not struct or struct pointer")
	}

	// Check if we already have an entity for this key.
	if ke := ds.findKeyEntity(key); ke == nil {
		// Key doesn't exist so add it.
		ds.keyEntities = append(ds.keyEntities, &keyEntity{
			key:    key,
			entity: val.Interface(), // Make sure we capture the value not ptr.
		})
	} else {
		// Key already exists so just update the entity.
		ke.entity = val.Interface() // Make sure we capture the value not ptr.
	}

	return key, nil
}

func (ds *ds) Delete(keys []datastore.Key) error {

	for _, key := range keys {
		if err := ds.del(key); err != nil {
			return err
		}
	}
	return nil
}

func (ds *ds) del(key datastore.Key) error {

	// Find the key entity and delete it from slice of key entities.
	for i, ke := range ds.keyEntities {
		if ke.key.Equal(key) {
			// This slice element delete will leak memory but is simple. See
			// https://goo.gl/4Eer5r for a solution if this becomes a problem.
			ds.keyEntities = append(ds.keyEntities[:i], ds.keyEntities[i+1:]...)
			break
		}
	}
	return nil
}

// compareValues compares according to App Engine comparators.
func compareValues(left, right interface{}) int {

	// The order in which the App Engine datastore compares types.
	comp := 0
	switch left.(type) {
	case int64:
		comp = -4
	case string:
		comp = -3
	case float64:
		comp = -2
	case datastore.Key:
		comp = -1
	default:
		panic("unknown property type")
	}

	switch right.(type) {
	case int64:
		comp = comp + 4
	case string:
		comp = comp + 3
	case float64:
		comp = comp + 2
	case datastore.Key:
		comp = comp + 1
	default:
		panic("unknown property type")
	}

	if comp < 0 {
		return -1
	} else if comp > 0 {
		return 1
	}

	// We know the left type is the same as the right as comp == 0 so now
	// compare the values of each type.
	switch left.(type) {
	case string:
		return strings.Compare(left.(string), right.(string))
	case int64:
		l, r := left.(int64), right.(int64)
		if l < r {
			return -1
		} else if l > r {
			return 1
		}
		return 0
	case float64:
		l, r := left.(float64), right.(float64)
		if l < r {
			return -1
		} else if l > r {
			return 1
		}
		return 0
	case datastore.Key:
		return compareKeys(left.(datastore.Key), right.(datastore.Key))
	default:
		panic("unknown property type")
	}
}

func compareKeys(left, right datastore.Key) int {

	// Keys with more parents come before those with less.
	leftParentCount := 0
	for parent := left.Parent(); parent != nil; parent = parent.Parent() {
		leftParentCount++
	}

	rightParentCount := 0
	for parent := right.Parent(); parent != nil; parent = parent.Parent() {
		rightParentCount++
	}

	if leftParentCount > rightParentCount {
		return -1
	} else if leftParentCount < rightParentCount {
		return 1
	}

	// From now on we know both keys have the same number of parents.

	// Order by IDs.
	leftID := left.ID()
	rightID := right.ID()

	comp := 0

	// Integer IDs always come before string IDs.
	switch leftID.(type) {
	case int64:
		comp = -2
	case string:
		comp = -1
	default:
		panic("unknown ID type")
	}

	switch rightID.(type) {
	case int64:
		comp = comp + 2
	case string:
		comp = comp + 1
	default:
		panic("unknown ID type")
	}

	if comp < 0 {
		return -1
	} else if comp > 0 {
		return 1
	}

	// Types are the same so compare by type.
	switch leftID.(type) {
	case int64:
		l, r := leftID.(int64), rightID.(int64)
		if l < r {
			return -1
		} else if l > r {
			return 1
		}

		// If IDs are identical then lets try comparing parent IDs.
		if left.Parent() != nil {
			return compareKeys(left.Parent(), right.Parent())
		}
		return 0
	case string:
		comp := strings.Compare(leftID.(string), rightID.(string))
		if comp != 0 {
			return comp
		}

		// If IDs are identical then lets try comparing parent IDs.
		if left.Parent() != nil {
			return compareKeys(left.Parent(), right.Parent())
		}
		return 0
	default:
		panic("unknown ID type")
	}
}

type keyEntitySorter struct {
	keyEntities []*keyEntity
	orders      []datastore.Order
}

func (s *keyEntitySorter) Len() int {
	return len(s.keyEntities)
}

func (s *keyEntitySorter) Swap(i, j int) {
	s.keyEntities[i], s.keyEntities[j] = s.keyEntities[j], s.keyEntities[i]
}

func (s *keyEntitySorter) Less(l, r int) bool {
	lke := s.keyEntities[l]
	rke := s.keyEntities[r]

	leftEntity := reflect.ValueOf(lke.entity)
	rightEntity := reflect.ValueOf(rke.entity)

	for _, o := range s.orders {

		// Compare entity keys.
		if o.Name == datastore.KeyName {
			comp := compareKeys(lke.key, rke.key)
			if comp < 0 {
				return o.Dir == datastore.AscDir
			} else if comp > 0 {
				return o.Dir == datastore.DescDir
			}
			continue
		}

		// Compare entity properties.

		var leftVal interface{}

		// Does the left field exist and is it exported.
		leftStructField, hasLeftField := leftEntity.Type().FieldByName(o.Name)
		if hasLeftField && leftStructField.PkgPath == "" {
			leftVal = leftEntity.FieldByName(o.Name).Interface()
		}

		var rightVal interface{}

		// Does the right field exist and is it exported.
		rightStructField, hasRightField := rightEntity.Type().FieldByName(
			o.Name)
		if hasRightField && rightStructField.PkgPath == "" {
			rightVal = rightEntity.FieldByName(o.Name).Interface()
		}

		switch {
		case leftVal == nil && rightVal == nil:
			return false
		case leftVal == nil:
			return true
		case rightVal == nil:
			return false
		default:
			comp := compareValues(leftVal, rightVal)
			if comp < 0 {
				return o.Dir == datastore.AscDir
			} else if comp > 0 {
				return o.Dir == datastore.DescDir
			}
			// Loop around to the next sort order if possible as properties are
			// equal at this point.
		}
	}

	// Values are at least equal.
	return false
}

func (ds *ds) AllocateKeys(key datastore.Key, n int) ([]datastore.Key, error) {
	baseKey := key.Parent()
	if baseKey == nil {
		baseKey = datastore.NewKey(key.Namespace())
	}

	keys := make([]datastore.Key, n)
	for i := range keys {
		keys[i] = baseKey.IntID(key.Kind(), ds.nextIntID())
	}
	return keys, nil
}

func (ds *ds) Run(q datastore.Query) (datastore.Iterator, error) {

	indexesToRemove := map[int]struct{}{}

	// Find entites to remove from our final iteration result.
	for i, ke := range ds.keyEntities {
		if q.Namespace != ke.key.Namespace() {
			indexesToRemove[i] = struct{}{}
		}

		if q.Kind == "" {
			// Don't filter on kind if it is empty.
			continue
		} else if ke.key.Kind() != q.Kind {
			indexesToRemove[i] = struct{}{}
		}

		for _, f := range q.Filters {

			if err := validateFilterValue(f.Value); err != nil {
				return nil, err
			}

			var propValue interface{}

			if f.Name == datastore.KeyName {
				// Filter by entity key.
				propValue = ke.key
			} else if _, exists := reflect.TypeOf(
				ke.entity).FieldByName(f.Name); exists {
				// Filter by entity property.
				propValue = reflect.ValueOf(
					ke.entity).FieldByName(f.Name).Interface()
			} else {
				// No property to filter on so continue to next filter.
				continue
			}

			comp := compareValues(propValue, f.Value)

			// TODO: Expand this.
			switch f.Op {
			case datastore.EqualOp:
				if comp != 0 {
					indexesToRemove[i] = struct{}{}
				}
			}
		}
	}

	keyEntities := []*keyEntity{}
	for i, ke := range ds.keyEntities {
		if _, remove := indexesToRemove[i]; remove {
			continue
		}
		keyEntities = append(keyEntities, ke)
	}

	// Execute orders.
	sort.Sort(&keyEntitySorter{
		keyEntities: keyEntities,
		orders:      q.Orders,
	})

	return &iterator{
		keyEntities: keyEntities,
		keysOnly:    q.KeysOnly,
	}, nil
}

func validateFilterValue(value interface{}) error {
	switch value.(type) {
	case int64, float64, datastore.Key:
		return nil
	default:
		return errors.New("unknown filter value type")
	}
}

type iterator struct {
	keyEntities []*keyEntity
	keysOnly    bool

	index int
}

func (it *iterator) Next(entity interface{}) (datastore.Key, error) {

	// Check to see if there are on more entities to return.
	if it.index >= len(it.keyEntities) {
		if entity == nil {
			return nil, nil
		}

		// Zero the entity if there is nothing left like App Engine does.
		val, err := extractStruct(entity)
		if err != nil {
			return nil, err
		}
		val.Set(reflect.Zero(val.Type()))

		return nil, nil
	}

	keyEntity := it.keyEntities[it.index]
	it.index++

	if it.keysOnly {
		return keyEntity.key, nil
	}

	val, err := extractStruct(entity)
	if err != nil {
		return nil, err
	}
	val.Set(reflect.ValueOf(keyEntity.entity))
	return keyEntity.key, nil
}

func (ds *ds) RunInTransaction(f func(datastore.Datastore) error) error {
	txDs := &txDs{
		ds: ds,
	}
	if err := f(txDs); err != nil {
		return err
	}
	for _, m := range txDs.mutators {
		if err := m(ds); err != nil {
			return err
		}
	}
	return nil
}

type txDs struct {
	ds       *ds
	mutators []func(ds datastore.Datastore) error
}

func (ds *txDs) Get(keys []datastore.Key, entities interface{}) error {
	return ds.ds.Get(keys, entities)
}

func (ds *txDs) Put(keys []datastore.Key, entities interface{}) ([]datastore.Key, error) {

	// Return complete keys witin the transaction by automatically completing
	// them even though ds.Put isn't actually called yet.
	completeKeys := make([]datastore.Key, len(keys))
	for i, k := range keys {
		completeKey := k
		if k.Incomplete() {
			baseKey := k.Parent()
			if baseKey == nil {
				baseKey = datastore.NewKey(k.Namespace())
			}
			completeKey = baseKey.IntID(k.Kind(), ds.ds.nextIntID())

		}
		completeKeys[i] = completeKey
	}

	ds.mutators = append(ds.mutators, func(ds datastore.Datastore) error {
		_, err := ds.Put(completeKeys, entities)
		return err
	})
	return completeKeys, nil
}

func (ds *txDs) Delete(keys []datastore.Key) error {
	ds.mutators = append(ds.mutators, func(ds datastore.Datastore) error {
		return ds.Delete(keys)
	})
	return nil
}

func (ds *txDs) AllocateKeys(key datastore.Key, n int) ([]datastore.Key, error) {
	return ds.ds.AllocateKeys(key, n)
}

func (ds *txDs) Run(q datastore.Query) (datastore.Iterator, error) {
	return nil, errors.New("not implemented")
}
