package datastore

import (
	"reflect"
	"strings"
)

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
