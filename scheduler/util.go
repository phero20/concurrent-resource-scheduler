package scheduler

import (
	"reflect"
)

// isNil checks if a resource is nil. It skips reflection entirely for non-nillable
// types (like int or struct) by using the precomputed isNillable flag.
func (s *Scheduler[T, ID]) isNil(res T) bool {
	if !s.isNillable {
		return false
	}
	v := any(res)
	if v == nil {
		return true
	}
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return val.IsNil()
	}
	return false
}
