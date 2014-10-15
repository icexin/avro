# rules
## Primitive Types
- null is avro.Null.
- bool is bool.
- int and long as int int32 ...
- float and double is float32 and float64.
- bytes is []byte.
- string is string.

## Complex Types
- record is struct. when decoding, field type can not be interface{}.
- enums is int.
- array is slice.
- map is map. when decoding, value of map can not be interface{}
- fixed is array.
- unions is avro.Union.
