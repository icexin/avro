package avro

import (
        "bytes"
        "reflect"
        "testing"
)

func RunCase(t *testing.T, c interface{}) {
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)
        dec := NewDecoder(buf)
        v := reflect.ValueOf(c)
        for i := 0; i < v.Len(); i++ {
                item := v.Index(i)
                if err := enc.Encode(item.Interface()); err != nil {
                        t.Error(err)
                }
                d := reflect.New(item.Type())
                tp := d.Elem().Type()
                switch tp.Kind() {
                case reflect.Map:
                        d.Elem().Set(reflect.MakeMap(tp))
                case reflect.Slice:
                        d.Elem().Set(reflect.MakeSlice(tp, 0, 0))
                }
                if err := dec.Decode(d.Interface()); err != nil {
                        t.Error(err)
                }
                if reflect.DeepEqual(reflect.Indirect(d), v) {
                        t.Error(d)
                }
        }
}

func TestDecodeNil(t *testing.T) {
        RunCase(t, []Null{Null(0)})
}

func TestDecodeInt(t *testing.T) {
        RunCase(t, []int{0, 1, 1, 2, 3, 5})
        RunCase(t, []int32{0, 1, 1, 2, 3, 5})
        RunCase(t, []int64{0, 1, 1, 2, 3, 5})
        RunCase(t, []uint32{0, 1, 1, 2, 3, 5})
        RunCase(t, []uint64{0, 1, 1, 2, 3, 5})
}

func TestDecodeFloat(t *testing.T) {
        RunCase(t, []float32{0, 1, 1, 2, 3, 5})
        RunCase(t, []float64{0, 1, 1, 2, 3, 5})
}

func TestDecodeBool(t *testing.T) {
        RunCase(t, []bool{true, false})
}

func TestDecodeString(t *testing.T) {
        RunCase(t, []string{"", "a", "abc"})
}

func TestDecodeFixed(t *testing.T) {
        RunCase(t, [][3]byte{[3]byte{1, 2, 3}})
}

func TestDecodeArray(t *testing.T) {
        RunCase(t, [][]string{
                []string{"", "a", "ab"},
                []string{"1234", "a", "ab"},
        })
}

func TestDecodeMap(t *testing.T) {
        RunCase(t, []map[string]string{
                map[string]string{},
                map[string]string{"key": "value"},
        })
}

type record struct {
        Int    int
        Nil    Null
        skip   int
        Fixed  [3]byte
        String string
}

func TestDecodeStruct(t *testing.T) {
        RunCase(t, []record{
                record{
                        1,
                        Null(0),
                        2,
                        [3]byte{1, 2, 3},
                        "abc",
                },
        })
}

func TestDecodeUnion(t *testing.T) {
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)
        dec := NewDecoder(buf)
        uni := Union{
                Idx: 1,
                Elem: []interface{}{
                        Null(0),
                        "abc",
                },
        }
        if err := enc.Encode(uni); err != nil {
                t.Fatal(err)
        }

        uni1 := Union{
                Elem: []interface{}{
                        Null(0),
                        new(string),
                },
        }

        if err := dec.Decode(&uni1); err != nil {
                t.Fatal(err)
        }

        if uni1.Idx != 1 {
                t.Fatal(uni1.Idx)
        }
        s := uni1.Elem[1].(*string)
        if *s != "abc" {
                t.Fatal(*s)
        }
}
