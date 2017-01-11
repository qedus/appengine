package memds

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/qedus/ds"
	"golang.org/x/net/context"
)

/*
type notFoundError map[int]bool

func (nfe notFoundError) Error() string {
	return "entities not found"
}

func (nfe notFoundError) NotFound(index int) bool {
	return nfe[index]
}

*/
type keyEntity struct {
	key    ds.Key
	entity interface{}
}

/*
type keyValue struct {
	key   datastore.Key
	value interface{}
}
*/

type memDs struct {
	keyEntities []keyEntity
	lastIntID   int64
}

// New creates a new TransationalDatastore that resides solely in memory. It is
// useful for fast unit testing datastore code compared to using
// google.golang.org/appengine/aetest.
func New() ds.Ds {
	return &memDs{
		keyEntities: []keyEntity{},
	}
}

func (mds *memDs) nextIntID() int64 {
	mds.lastIntID++
	return mds.lastIntID
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

func (mds *memDs) Get(ctx context.Context,
	keys []ds.Key, entities interface{}) error {

	values := reflect.ValueOf(entities)

	if err := verifyKeysValues(keys, values); err != nil {
		return err
	}

	sparseErrs := make(map[int]error)
	for i, key := range keys {
		value := values.Index(i)

		found, err := mds.get(key, value.Interface())
		if err != nil {
			sparseErrs[i] = err
			continue
		}
		if !found {
			sparseErrs[i] = ds.ErrNoEntity
		}
	}

	if len(sparseErrs) == 0 {
		return nil
	}

	errs := make(ds.Error, len(keys))
	for i, err := range sparseErrs {
		errs[i] = err
	}

	return errs
}

func (mds *memDs) get(key ds.Key, entity interface{}) (bool, error) {

	val, err := extractStruct(entity)
	if err != nil {
		return false, err
	}

	ke := mds.findKeyEntity(key)
	if ke == nil {
		return false, nil
	}
	val.Set(reflect.ValueOf(ke.entity))

	return true, nil
}

func (mds *memDs) findKeyEntity(key ds.Key) *keyEntity {
	for _, ke := range mds.keyEntities {
		if ke.key.Equal(key) {
			return &ke
		}
	}
	return nil
}

func verifyKeysValues(keys []ds.Key, values reflect.Value) error {
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

func (mds *memDs) Put(ctx context.Context,
	keys []ds.Key, entities interface{}) ([]ds.Key, error) {
	values := reflect.ValueOf(entities)

	if err := verifyKeysValues(keys, values); err != nil {
		return nil, err
	}

	completeKeys := make([]ds.Key, len(keys))
	for i, key := range keys {
		val := values.Index(i)
		completeKey, err := mds.put(key, val.Interface())
		if err != nil {
			return nil, err
		}
		completeKeys[i] = completeKey
	}

	return completeKeys, nil
}

func (mds *memDs) put(key ds.Key, entity interface{}) (ds.Key, error) {

	// If key is incomplete then complete it.
	keyIsIncomplete := key.Path[len(key.Path)-1].ID == nil
	if keyIsIncomplete {
		key.Path[len(key.Path)-1].ID = mds.nextIntID()
	}

	val := reflect.ValueOf(entity)
	switch val.Kind() {
	case reflect.Ptr:
		val = val.Elem()
		if val.Kind() != reflect.Struct {
			return ds.Key{},
				errors.New("memds: entity not struct or struct pointer")
		}
	case reflect.Struct:
		// Allowed entity kind.
	default:
		return ds.Key{},
			errors.New("memds: entity not struct or struct pointer")
	}

	// Ensure all fields are zeroed if asked to do so by the struct tags.
	for i := 0; i < val.NumField(); i++ {
		fieldVal := reflect.Indirect(val.Field(i))

		fieldStruct := val.Type().Field(i)
		if propertyName(fieldStruct) == "" {
			fieldVal.Set(reflect.Zero(fieldVal.Type()))
		}
	}

	// Check if we already have an entity for this key.
	if ke := mds.findKeyEntity(key); ke == nil {
		// Key doesn't exist so add it.
		mds.keyEntities = append(mds.keyEntities, keyEntity{
			key:    key,
			entity: val.Interface(), // Make sure we capture the value not ptr.
		})
	} else {
		// Key already exists so just update the entity.
		ke.entity = val.Interface() // Make sure we capture the value not ptr.
	}

	return key, nil
}

func (mds *memDs) Delete(ctx context.Context, keys []ds.Key) error {

	for _, key := range keys {
		if err := mds.del(key); err != nil {
			return err
		}
	}
	return nil
}

func (mds *memDs) del(key ds.Key) error {

	// Find the key entity and delete it from slice of key entities.
	for i, ke := range mds.keyEntities {
		if ke.key.Equal(key) {
			// This slice element delete will leak memory but is simple. See
			// https://goo.gl/4Eer5r for a solution if this becomes a problem.
			mds.keyEntities = append(mds.keyEntities[:i],
				mds.keyEntities[i+1:]...)
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
		comp = -6
	case time.Time:
		comp = -5
	case bool:
		comp = -4
	case string:
		comp = -3
	case float64:
		comp = -2
	case ds.Key:
		comp = -1
	default:
		panic("unknown property type")
	}

	switch right.(type) {
	case int64:
		comp = comp + 6
	case time.Time:
		comp = comp + 5
	case bool:
		comp = comp + 4
	case string:
		comp = comp + 3
	case float64:
		comp = comp + 2
	case ds.Key:
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
	case bool:
		l, r := left.(bool), right.(bool)
		if !l && r {
			return -1
		} else if l && !r {
			return 1
		}
		return 0
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
	case ds.Key:
		return compareKeys(left.(ds.Key), right.(ds.Key))
	case time.Time:
		l, r := left.(time.Time), right.(time.Time)
		if l.Before(r) {
			return -1
		} else if l.After(r) {
			return 1
		}
		return 0
	default:
		panic("unknown property type")
	}
}

func compareKeys(left, right ds.Key) int {

	// Keys with more parents come before those with less.
	leftParentCount := len(left.Path) - 1
	rightParentCount := len(right.Path) - 1

	if leftParentCount > rightParentCount {
		return -1
	} else if leftParentCount < rightParentCount {
		return 1
	}

	// From now on we know both keys have the same number of parents.

	// Order by IDs.
	leftID := left.Path[len(left.Path)-1].ID
	rightID := right.Path[len(right.Path)-1].ID

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
		if len(left.Path) > 1 {
			leftParent := left
			leftParent.Path = left.Path[:len(left.Path)-1]

			rightParent := right
			rightParent.Path = right.Path[:len(right.Path)-1]

			return compareKeys(leftParent, rightParent)
		}
		return 0
	case string:
		comp := strings.Compare(leftID.(string), rightID.(string))
		if comp != 0 {
			return comp
		}

		// If IDs are identical then lets try comparing parent IDs.
		if len(left.Path) > 1 {
			leftParent := left
			leftParent.Path = left.Path[:len(left.Path)-1]

			rightParent := right
			rightParent.Path = right.Path[:len(right.Path)-1]

			return compareKeys(leftParent, rightParent)
		}

		return 0
	default:
		panic("unknown ID type")
	}
}

type keyEntitySorter struct {
	keyEntities []keyEntity
	orders      []ds.Order
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
		// TODO: Remove hard coding here.
		if o.Name == "__key__" {
			comp := compareKeys(lke.key, rke.key)
			if comp < 0 {
				return o.Dir == ds.AscDir
			} else if comp > 0 {
				return o.Dir == ds.DescDir
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
				return o.Dir == ds.AscDir
			} else if comp > 0 {
				return o.Dir == ds.DescDir
			}
			// Loop around to the next sort order if possible as properties are
			// equal at this point.
		}
	}

	// Values are at least equal.
	return false
}

func (mds *memDs) AllocateKeys(ctx context.Context, parent ds.Key, n int) (
	[]ds.Key, error) {

	lastPathElement := len(parent.Path) - 1
	keys := make([]ds.Key, n)
	for i := range keys {
		keys[i] = parent
		keys[i].Path[lastPathElement].ID = mds.nextIntID()
	}
	return keys, nil
}

func findFieldName(entity interface{}, fieldOrTagName string) string {

	ty := reflect.TypeOf(entity)
	if _, exists := ty.FieldByName(fieldOrTagName); exists {
		return fieldOrTagName
	}

	// Field name doesn't exist so see if it maps to a user defined tag name.
	for i := 0; i < ty.NumField(); i++ {
		field := ty.Field(i)
		propName := propertyName(field)
		if propName == fieldOrTagName {
			return field.Name
		}
	}

	// No field found with specific name.
	return ""
}

func isAncestor(parent, child ds.Key) bool {

	// It's not possible for a parent path to be longer than a child's.
	if len(parent.Path) > len(child.Path) {
		return false
	}

	// Chop the extra parts of the child.
	child.Path = child.Path[:len(parent.Path)]
	child.Path[len(child.Path)-1].ID = nil

	return parent.Equal(child)
}

func isComparisonTrue(left interface{},
	op ds.FilterOp, right interface{}) bool {

	comp := compareValues(left, right)

	switch op {
	case ds.LessThanOp:
		return comp < 0
	case ds.LessThanEqualOp:
		return comp <= 0
	case ds.EqualOp:
		return comp == 0
	case ds.GreaterThanEqualOp:
		return comp >= 0
	case ds.GreaterThanOp:
		return comp > 0
	default:
		panic("unknown filter op")
	}
}

func isIndexableSlice(propValue interface{}) bool {
	ty := reflect.TypeOf(propValue)
	if ty.Kind() != reflect.Slice {
		return false
	}

	// Check this propValue slice isn't a []byte.
	return ty.Elem().Kind() != reflect.Uint8
}

func (mds *memDs) Run(ctx context.Context, q ds.Query) (ds.Iterator, error) {

	indexesToRemove := map[int]struct{}{}

	// Find entites to remove from our final iteration result.
	for i, ke := range mds.keyEntities {
		if q.Root.Namespace != ke.key.Namespace {
			indexesToRemove[i] = struct{}{}
		}

		rootKind := q.Root.Path[len(q.Root.Path)-1].Kind
		keyKind := ke.key.Path[len(ke.key.Path)-1].Kind
		if rootKind == "" {
			// Don't filter on kind if it is empty.
			continue
		} else if keyKind != rootKind {
			indexesToRemove[i] = struct{}{}
		}

		// Remove non-ancestors.
		if !isAncestor(q.Root, ke.key) {
			indexesToRemove[i] = struct{}{}
		}

		for _, f := range q.Filters {

			if err := validateFilterValue(f.Value); err != nil {
				return nil, err
			}

			var propValue interface{}

			// TOTO: Hard code this.
			if f.Name == "__key__" {
				// Filter by entity key.
				propValue = ke.key
			} else if fieldName := findFieldName(
				ke.entity, f.Name); fieldName != "" {
				propValue = reflect.ValueOf(
					ke.entity).FieldByName(fieldName).Interface()
			} else {
				// No property to filter on so continue to next filter.
				continue
			}

			// Cater for entity property slices. If any of the elements in a
			// slice is not a filter match then don't remove the entity from
			// the iteration candidates. Note that a []byte is not indexable
			// and is treated as a single property.
			if isIndexableSlice(propValue) {
				shouldRemove := true
				v := reflect.ValueOf(propValue)
				for j := 0; j < v.Len(); j++ {
					if isComparisonTrue(v.Index(j).Interface(),
						f.Op, f.Value) {
						shouldRemove = false
						break
					}
				}
				if shouldRemove {
					indexesToRemove[i] = struct{}{}
				}
			} else {
				if !isComparisonTrue(propValue, f.Op, f.Value) {
					indexesToRemove[i] = struct{}{}
				}
			}
		}
	}

	keyEntities := []keyEntity{}
	for i, ke := range mds.keyEntities {
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
	case int, int64, float64, string:
		return nil
	default:
		return fmt.Errorf("unsupported filter value type %T", value)
	}
}

type iterator struct {
	keyEntities []keyEntity
	keysOnly    bool

	index int
}

func (it *iterator) Next(entity interface{}) (ds.Key, error) {

	// Check to see if there are on more entities to return.
	if it.index >= len(it.keyEntities) {
		if entity == nil {
			return ds.Key{}, nil
		}

		// Zero the entity if there is nothing left like App Engine does.
		val, err := extractStruct(entity)
		if err != nil {
			return ds.Key{}, err
		}
		val.Set(reflect.Zero(val.Type()))

		return ds.Key{}, nil
	}

	keyEntity := it.keyEntities[it.index]
	it.index++

	if it.keysOnly {
		return keyEntity.key, nil
	}

	val, err := extractStruct(entity)
	if err != nil {
		return ds.Key{}, err
	}
	val.Set(reflect.ValueOf(keyEntity.entity))
	return keyEntity.key, nil
}

func (mds *memDs) RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	txDs := &txDs{
		ds: mds,
	}

	tctx := ds.NewContext(ctx, txDs)
	if err := f(tctx); err != nil {
		return err
	}
	for _, m := range txDs.mutators {
		if err := m(ctx, mds); err != nil {
			return err
		}
	}
	return nil
}

type txDs struct {
	ds       *memDs
	mutators []func(context.Context, ds.Ds) error
}

func (tds *txDs) RunInTransaction(ctx context.Context,
	f func(context.Context) error) error {
	return errors.New("already in transaction")
}

func (tds *txDs) Get(ctx context.Context,
	keys []ds.Key, entities interface{}) error {
	return tds.ds.Get(ctx, keys, entities)
}

func (tds *txDs) Put(ctx context.Context, keys []ds.Key, entities interface{}) (
	[]ds.Key, error) {

	// Return complete keys witin the transaction by automatically completing
	// them even though ds.Put isn't actually called yet.
	completeKeys := make([]ds.Key, len(keys))
	for i, key := range keys {
		keyIsIncomplete := key.Path[len(key.Path)-1].ID == nil
		if keyIsIncomplete {
			key.Path[len(key.Path)-1].ID = tds.ds.nextIntID()
		}
		completeKeys[i] = key
	}

	tds.mutators = append(tds.mutators,
		func(ctx context.Context, ds ds.Ds) error {
			_, err := ds.Put(ctx, completeKeys, entities)
			return err
		})
	return completeKeys, nil
}

func (tds *txDs) Delete(ctx context.Context, keys []ds.Key) error {
	tds.mutators = append(tds.mutators,
		func(ctx context.Context, ds ds.Ds) error {
			return ds.Delete(ctx, keys)
		})
	return nil
}

func (tds *txDs) AllocateKeys(ctx context.Context, parent ds.Key, n int) (
	[]ds.Key, error) {
	return tds.ds.AllocateKeys(ctx, parent, n)
}

func (tds *txDs) Run(ctx context.Context, q ds.Query) (ds.Iterator, error) {
	return nil, errors.New("not implemented")
}

func propertyName(field reflect.StructField) string {

	// Don't include unexported fields.
	if field.PkgPath != "" {
		return ""
	}

	// See if the user has a specific name they would like to use for the field.
	tagValues := strings.Split(field.Tag.Get("datastore"), ",")
	if len(tagValues) > 0 {
		switch tagValues[0] {
		case "-":
			// This field isn't needed.
			return ""
		case "":
			return field.Name
		default:
			return tagValues[0]
		}
	}
	return field.Name
}
