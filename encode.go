package avro

import (
        "avro/zigzag"
        "bytes"
        "encoding/binary"
        "errors"
        "fmt"
        "io"
        "reflect"
)

func Marshal(x interface{}) ([]byte, error) {
        var buf bytes.Buffer
        enc := NewEncoder(&buf)
        err := enc.Encode(x)
        if err != nil {
                return nil, err
        }
        return buf.Bytes(), nil
}

type Encoder struct {
        w   io.Writer
        buf *bytes.Buffer
        b   [10]byte
}

func NewEncoder(w io.Writer) *Encoder {
        return &Encoder{
                w:   w,
                buf: new(bytes.Buffer),
        }
}

func (e *Encoder) Encode(x interface{}) error {
        err := e.marshal(x)
        if err != nil {
                e.buf.Reset()
                return err
        }
        _, err = e.buf.WriteTo(e.w)
        return err
}

func (e *Encoder) marshal(x interface{}) error {
        if x == nil {
                return nil
        }
        switch v := x.(type) {
        case Null, *Null:
                return nil
        case bool:
                if v {
                        binary.Write(e.buf, binary.BigEndian, byte(1))
                } else {
                        binary.Write(e.buf, binary.BigEndian, byte(0))
                }
        case int, int8, int16, int32, int64:
                n := binary.PutUvarint(e.b[:], zigzag.Encode(reflect.ValueOf(v).Int()))
                e.buf.Write(e.b[:n])
        case uint, uint8, uint16, uint32, uint64:
                n := binary.PutUvarint(e.b[:], zigzag.Encode(int64(reflect.ValueOf(v).Uint())))
                e.buf.Write(e.b[:n])

        case float32, float64:
                binary.Write(e.buf, binary.BigEndian, v)
        case []byte:
                e.marshal(len(v))
                e.buf.Write(v)
        case string:
                e.marshal([]byte(v))
        default:
                return e.marshalComlpex(x)
        }
        return nil
}

func (e *Encoder) marshalComlpex(x interface{}) error {
        p := reflect.ValueOf(x)
        v := reflect.Indirect(p)
        t := v.Type()
        // for union
        if u, ok := x.(Union); ok {
                if len(u.Elem) <= u.Idx {
                        return errors.New("union index error")
                }
                e.marshal(u.Idx)
                return e.marshal(u.Elem[u.Idx])
        }
        // for enum use int instead
        // for fixed use array
        switch v.Kind() {
        case reflect.Array:
                if t.Elem().Kind() != reflect.Uint8 {
                        return errors.New("element of array must be byte")
                }
                binary.Write(e.buf, binary.BigEndian, x)
        // for array use slice
        case reflect.Slice:
                if v.Len() > 0 {
                        e.marshal(v.Len())
                        for i := 0; i < v.Len(); i++ {
                                err := e.marshal(v.Index(i).Interface())
                                if err != nil {
                                        return err
                                }
                        }
                }
                e.marshal(0)
        // for map
        case reflect.Map:
                if t.Key().Kind() != reflect.String {
                        return errors.New("map key must be string")
                }
                if v.Len() > 0 {
                        e.marshal(v.Len())
                        keys := v.MapKeys()
                        for _, k := range keys {
                                e.marshal(k.Interface())
                                err := e.marshal(v.MapIndex(k).Interface())
                                if err != nil {
                                        return err
                                }
                        }
                }
                e.marshal(0)
        // for record
        case reflect.Struct:
                n := t.NumField()
                for i := 0; i < n; i++ {
                        // unexported
                        if t.Field(i).PkgPath != "" {
                                continue
                        }
                        record := v.Field(i)
                        err := e.marshal(record.Interface())
                        if err != nil {
                                return err
                        }
                }
        default:
                panic(fmt.Errorf("not supported:%s", p.Type()))
        }
        return nil
}
