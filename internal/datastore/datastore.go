package datastore

import (
	"errors"
	"reflect"
	"strings"
	"time"

	eds "github.com/qedus/appengine/datastore"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	aeds "google.golang.org/appengine/datastore"
)

type notFoundError map[int]bool

func (nfe notFoundError) Error() string {
	return "entities not found"
}

func (nfe notFoundError) NotFound(index int) bool {
	return nfe[index]
}

type datastore struct {
	ctx context.Context

	get              func(context.Context, []*aeds.Key, interface{}) error
	put              func(context.Context, []*aeds.Key, interface{}) ([]*aeds.Key, error)
	del              func(context.Context, []*aeds.Key) error
	runInTransaction func(context.Context, func(context.Context) error) error
}

type Config struct {
	Get              func(context.Context, []*aeds.Key, interface{}) error
	Put              func(context.Context, []*aeds.Key, interface{}) ([]*aeds.Key, error)
	Delete           func(context.Context, []*aeds.Key) error
	RunInTransaction func(context.Context, func(context.Context) error) error
}

// New returns a new TransactionalDatastore service that can be used to interact
// with the App Engine production and development SDK datastores.
func New(ctx context.Context, cfg Config) eds.TransactionalDatastore {
	return &datastore{
		ctx: ctx,

		get:              cfg.Get,
		put:              cfg.Put,
		del:              cfg.Delete,
		runInTransaction: cfg.RunInTransaction,
	}
}

func (ds *datastore) toAEKey(key eds.Key) (*aeds.Key, error) {
	// Prevent infinite recursion when key is nil.
	if key == nil {
		return nil, nil
	}

	kind := key.Kind()
	parent, err := ds.toAEKey(key.Parent())
	if err != nil {
		return nil, err
	}

	ctx, err := appengine.Namespace(ds.ctx, key.Namespace())
	if err != nil {
		return nil, err
	}

	switch id := key.ID().(type) {
	case string:
		return aeds.NewKey(ctx, kind, id, 0, parent), nil
	case int64:
		return aeds.NewKey(ctx, kind, "", id, parent), nil
	case nil:
		return aeds.NewIncompleteKey(ctx, kind, parent), nil
	}
	return nil, errors.New("unknown key ID type")
}

func toKey(aeKey *aeds.Key) eds.Key {
	namespace := aeKey.Namespace()

	// Collect the entire key path.
	aeKeys := []*aeds.Key{aeKey}
	for {
		aeKey = aeKey.Parent()
		if aeKey == nil {
			break
		}
		aeKeys = append(aeKeys, aeKey)
	}

	// Replay the keys in ancestor order first.
	key := eds.NewKey(namespace)
	for i := len(aeKeys) - 1; i >= 0; i-- {
		aeKey := aeKeys[i]

		if aeKey.StringID() == "" {
			// An int id.
			key = key.IntID(aeKey.Kind(), aeKey.IntID())
		} else {
			// A string id.
			key = key.StringID(aeKey.Kind(), aeKey.StringID())
		}
	}
	return key
}

func (ds *datastore) valueToPropertyList(value reflect.Value) (
	aeds.PropertyList, error) {
	ty := value.Type()

	// Get the underlying type if the value is an interface.
	if ty.Kind() == reflect.Interface {
		value = value.Elem()
	}

	// Make sure we have the struct, not the pointer.
	value = reflect.Indirect(value)
	ty = value.Type()

	pl := make(aeds.PropertyList, 0, ty.NumField())

	for i := 0; i < ty.NumField(); i++ {
		structField := ty.Field(i)

		propName := PropertyName(structField)
		if propName == "" {
			// If there is no name then go on to the next field.
			continue
		}

		// Only include specific field types.
		var propValue interface{}
		switch structField.Type.Kind() {
		case reflect.Int64, reflect.String, reflect.Float64, reflect.Bool:
			propValue = value.Field(i).Interface()
		case reflect.Struct:
			switch v := value.Field(i).Interface().(type) {
			case time.Time:
				propValue = v
			default:
				continue
			}
		case reflect.Interface:
			// Check the interface is of Key type.
			key, ok := value.Field(i).Interface().(eds.Key)
			if !ok {
				// We currentlly don't allow any other type of interfaces.
				continue
			}

			aeKey, err := ds.toAEKey(key)
			if err != nil {
				return nil, err
			}
			propValue = aeKey
		case reflect.Slice:
			// Only accept certain types of slice.
			switch structField.Type.Elem().Kind() {
			case reflect.Int64, reflect.Float64, reflect.String:

				// Must convert the slice to a slice of properties.
				slice := value.Field(i)
				for i := 0; i < slice.Len(); i++ {
					pl = append(pl, aeds.Property{
						Name:     propName,
						Value:    slice.Index(i).Interface(),
						NoIndex:  PropertyNoIndex(structField),
						Multiple: true,
					})
				}
				continue
			case reflect.Uint8: // byte
				// Treat []byte as a standard property, not a multi property.
				propValue = value.Field(i).Interface()

				// Automatically set noindex.
				pl = append(pl, aeds.Property{
					Name:     propName,
					Value:    propValue,
					NoIndex:  true,
					Multiple: false,
				})
				continue

			default:
				continue
			}

		default:
			continue
		}

		pl = append(pl, aeds.Property{
			Name:     propName,
			Value:    propValue,
			NoIndex:  PropertyNoIndex(structField),
			Multiple: false,
		})

	}
	return pl, nil
}

func (ds *datastore) propertyListToValue(pl aeds.PropertyList,
	value reflect.Value) {
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}

	value = reflect.Indirect(value) // Make sure the value is a struct.
	valueType := value.Type()

	// Datastore property names are derived from struct field names or custom
	// struct tags. Map any tag renames to actual struct fields in order to get
	// the field value.
	fieldValues := make(map[string]reflect.Value, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := valueType.Field(i)

		propName := PropertyName(field)
		if propName == "" {
			// The struct user doesn't want this field or it is unexported.
			continue
		}
		fieldValues[propName] = value.Field(i)
	}

	multiProps := map[string]reflect.Value{}
	for _, p := range pl {

		// Is there a struct field that can take this property?
		v, exists := fieldValues[p.Name]
		if !exists {
			continue
		}

		if p.Multiple {
			if _, exists := multiProps[p.Name]; !exists {
				sliceType := reflect.SliceOf(reflect.TypeOf(p.Value))
				multiProps[p.Name] = reflect.MakeSlice(sliceType, 0, 1)
			}

			multiProps[p.Name] = reflect.Append(multiProps[p.Name],
				reflect.ValueOf(p.Value))
			continue
		}

		if p.Value == nil {
			continue
		}

		// Do any of the property values need to be transformed.
		propValue := p.Value

		switch v := propValue.(type) {
		case *aeds.Key:
			propValue = toKey(v)
		}

		v.Set(reflect.ValueOf(propValue))
	}

	for propName, propValues := range multiProps {
		fieldValue, exists := fieldValues[propName]
		if !exists {
			continue
		}

		fieldValue.Set(propValues)
	}
}

func (ds *datastore) Get(keys []eds.Key, entities interface{}) error {
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}

	pls := make([]aeds.PropertyList, len(keys))
	switch err := ds.get(ds.ctx, aeKeys, pls).(type) {
	case nil:
		values := reflect.ValueOf(entities)
		for i, pl := range pls {
			ds.propertyListToValue(pl, values.Index(i))
		}
		return nil
	case appengine.MultiError:
		nfe := notFoundError{}

		values := reflect.ValueOf(entities)
		for i, pl := range pls {
			switch err[i] {
			case nil:
				ds.propertyListToValue(pl, values.Index(i))
			case aeds.ErrNoSuchEntity:
				nfe[i] = true
			default:
				return err[i]
			}
		}

		return nfe
	default:
		return err
	}
}

func (ds *datastore) Delete(keys []eds.Key) error {
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return err
		}
		aeKeys[i] = aeKey
	}
	return ds.del(ds.ctx, aeKeys)
}

func verifyKeysValues(keys []eds.Key, values reflect.Value) error {
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

func (ds *datastore) Put(keys []eds.Key, entities interface{}) ([]eds.Key, error) {

	values := reflect.ValueOf(entities)
	if err := verifyKeysValues(keys, values); err != nil {
		return nil, err
	}

	// Convert keys to App Engine keys.
	aeKeys := make([]*aeds.Key, len(keys))
	for i, key := range keys {
		aeKey, err := ds.toAEKey(key)
		if err != nil {
			return nil, err
		}
		aeKeys[i] = aeKey
	}

	// Convert values to App Engine property lists so we can convert key
	// properties.
	pls := make([]aeds.PropertyList, values.Len())
	for i := 0; i < values.Len(); i++ {
		pl, err := ds.valueToPropertyList(values.Index(i))
		if err != nil {
			return nil, err
		}
		pls[i] = pl
	}

	completeAEKeys, err := ds.put(ds.ctx, aeKeys, pls)
	if err != nil {
		return nil, err
	}
	completeKeys := make([]eds.Key, len(completeAEKeys))
	for i, completeAEKey := range completeAEKeys {
		completeKeys[i] = toKey(completeAEKey)
	}
	return completeKeys, nil
}

func (ds *datastore) AllocateKeys(key eds.Key, n int) ([]eds.Key, error) {
	ctx, err := appengine.Namespace(ds.ctx, key.Namespace())
	if err != nil {
		return nil, err
	}
	parentKey, err := ds.toAEKey(key.Parent())
	if err != nil {
		return nil, err
	}

	low, _, err := aeds.AllocateIDs(ctx, key.Kind(), parentKey, n)
	if err != nil {
		return nil, err
	}

	baseKey := key.Parent()
	if baseKey == nil {
		baseKey = eds.NewKey(key.Namespace())
	}

	keys := make([]eds.Key, n)
	for i := range keys {
		keys[i] = baseKey.IntID(key.Kind(), low+int64(i))
	}
	return keys, nil
}

type iterator struct {
	ds   *datastore
	iter *aeds.Iterator
}

func (it *iterator) Next(entity interface{}) (eds.Key, error) {

	pl := aeds.PropertyList{}
	aeKey, err := it.iter.Next(&pl)
	if err == aeds.Done {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// Entity could be nil if keys only queries are used.
	if entity != nil {
		// Currently used to convert datastore.Keys to this packages keys.
		it.ds.propertyListToValue(pl, reflect.ValueOf(entity))
	}

	return toKey(aeKey), nil
}

func PropertyName(field reflect.StructField) string {

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

func PropertyNoIndex(field reflect.StructField) bool {

	tagValues := strings.Split(field.Tag.Get("datastore"), ",")
	if len(tagValues) > 1 {
		return tagValues[1] == "noindex"
	}
	return false
}

func (ds *datastore) Run(q eds.Query) (eds.Iterator, error) {
	aeQ := aeds.NewQuery(q.Kind)

	if q.Ancestor != nil {
		aeKey, err := ds.toAEKey(q.Ancestor)
		if err != nil {
			return nil, err
		}
		aeQ = aeQ.Ancestor(aeKey)
	}

	if q.KeysOnly {
		aeQ = aeQ.KeysOnly()
	}

	// Apply orders.
	for _, o := range q.Orders {
		var dirStr string
		switch o.Dir {
		case eds.AscDir:
			dirStr = ""
		case eds.DescDir:
			dirStr = "-"
		default:
			return nil, errors.New("unknown order dir")
		}
		aeQ = aeQ.Order(dirStr + o.Name)
	}

	// Apply fiters.
	for _, f := range q.Filters {
		var opStr string
		switch f.Op {
		case eds.LessThanOp:
			opStr = "<"
		case eds.LessThanEqualOp:
			opStr = "<="
		case eds.EqualOp:
			opStr = "="
		case eds.GreaterThanEqualOp:
			opStr = ">="
		case eds.GreaterThanOp:
			opStr = ">"
		default:
			return nil, errors.New("unknown filter op")
		}

		value := f.Value

		// Convert Key values to datastore.Keys.
		if key, ok := value.(eds.Key); ok {
			aeKey, err := ds.toAEKey(key)
			if err != nil {
				panic(err)
			}
			value = aeKey
		}

		aeQ = aeQ.Filter(f.Name+opStr, value)
	}

	ctx, err := appengine.Namespace(ds.ctx, q.Namespace)
	if err != nil {
		return nil, err
	}
	return &iterator{
		ds:   ds,
		iter: aeQ.Run(ctx),
	}, nil
}

func (ds *datastore) RunInTransaction(f func(eds.Datastore) error) error {
	return ds.runInTransaction(ds.ctx,
		func(tctx context.Context) error {
			return f(&datastore{
				ctx: tctx,

				get: ds.get,
				put: ds.put,
				del: ds.del,
			})
		})
}
