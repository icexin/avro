package avro

import (
        "avro/zigzag"
        "bufio"
        "bytes"
        "encoding/binary"
        "fmt"
        "io"
        "reflect"
)

func Unmarshal(b []byte, x interface{}) error {
        buf := bytes.NewBuffer(b)
        dec := NewDecoder(buf)
        return dec.Decode(x)
}

type Decoder struct {
        r *bufio.Reader
}

func NewDecoder(r io.Reader) *Decoder {
        return &Decoder{
                bufio.NewReader(r),
        }
}

func (d *Decoder) Decode(x interface{}) error {
        if x == nil {
                return nil
        }
        var err error
        switch v := x.(type) {
        case *Null:
                return nil
        case *bool:
                var n byte
                err = binary.Read(d.r, binary.BigEndian, &n)
                if err != nil {
                        break
                }
                if n == 0 {
                        *v = false
                } else {
                        *v = true
                }
        case *int, *int8, *int32, *int64:
                u, err := binary.ReadUvarint(d.r)
                if err != nil {
                        break
                }
                reflect.ValueOf(v).Elem().SetInt(zigzag.Decode(int64(u)))
        case *uint, *uint8, *uint32, *uint64:
                u, err := binary.ReadUvarint(d.r)
                if err != nil {
                        break
                }
                reflect.ValueOf(v).Elem().SetUint(uint64(zigzag.Decode(int64(u))))
        case *float32, *float64:
                err = binary.Read(d.r, binary.BigEndian, v)
        case *[]byte:
                var n int32
                err = d.Decode(&n)
                if err != nil {
                        break
                }
                *v = make([]byte, n)
                _, err = io.ReadFull(d.r, *v)
        case *string:
                var b []byte
                err = d.Decode(&b)
                if err != nil {
                        break
                }
                *v = string(b)
        default:
                err = d.decodeComplex(x)
        }
        return err
}

// for map and union
// for map, only support key and value are string
// for enum use int instead
// for fixed use array
// for array use slice
func (d *Decoder) decodeComplex(x interface{}) error {
        if _, ok := x.(*Union); ok {
                return d.decodeUnion(x)
        }

        p := reflect.ValueOf(x)
        if p.Kind() != reflect.Ptr {
                return fmt.Errorf("decodeComplex need ptr:%s", p.Type())
        }

        switch p.Elem().Kind() {
        case reflect.Array:
                return d.decodeArray(x)
        case reflect.Slice:
                return d.decodeSlice(x)
        case reflect.Map:
                return d.decodeMap(x)
        case reflect.Struct:
                return d.decodeStruct(x)
        default:
                panic(fmt.Errorf("not supported:%s", p.Type()))
        }
        return nil
}

func (d *Decoder) decodeUnion(x interface{}) error {
        v := x.(*Union)
        err := d.Decode(&v.Idx)
        if err != nil {
                return err
        }
        if len(v.Elem) <= v.Idx {
                return fmt.Errorf("union index error:%d", v.Idx)
        }
        return d.Decode(v.Elem[v.Idx])
}

func (d *Decoder) decodeArray(x interface{}) error {
        t := reflect.TypeOf(x).Elem()
        if t.Elem().Kind() != reflect.Uint8 {
                return fmt.Errorf("element of fixed must be byte:%s", t)
        }
        return binary.Read(d.r, binary.BigEndian, x)
}

func (d *Decoder) decodeMap(x interface{}) error {
        v := reflect.ValueOf(x).Elem()
        t := v.Type()
        if t.Key().Kind() != reflect.String {
                return fmt.Errorf("key of map must be string:%s", t.Key())
        }

        if v.IsNil() {
                v.Set(reflect.MakeMap(t))
        }

        var blkcnt int
        err := d.Decode(&blkcnt)
        if err != nil {
                return err
        }
        for blkcnt != 0 {
                if blkcnt < 0 {
                        blkcnt = -blkcnt
                        var n int
                        err = d.Decode(&n)
                        if err != nil {
                                return err
                        }
                }
                for i := 0; i < blkcnt; i++ {
                        key := reflect.New(t.Key())
                        err := d.Decode(key.Interface())
                        if err != nil {
                                return err
                        }
                        value := reflect.New(t.Elem())
                        err = d.Decode(value.Interface())
                        if err != nil {
                                return err
                        }
                        v.SetMapIndex(key.Elem(), value.Elem())
                }
                err = d.Decode(&blkcnt)
                if err != nil {
                        return err
                }

        }
        return nil
}

func (d *Decoder) decodeStruct(x interface{}) error {
        v := reflect.ValueOf(x).Elem()
        t := v.Type()
        n := t.NumField()
        for i := 0; i < n; i++ {
                f := v.Field(i)
                if f.CanSet() {
                        err := d.Decode(f.Addr().Interface())
                        if err != nil {
                                return fmt.Errorf("decode %s:%s", t.Field(i).Name, err)
                        }
                }
        }
        return nil
}

func (d *Decoder) decodeSlice(x interface{}) error {
        v := reflect.ValueOf(x).Elem()
        t := v.Type()
        if t.Elem().Kind() == reflect.Interface {
                return fmt.Errorf("element of slice must be concrete type, not interface")
        }
        if v.IsNil() {
                v.Set(reflect.MakeSlice(t, 0, 4))
        }

        var n int
        err := d.Decode(&n)
        if err != nil {
                return err
        }

        for n != 0 {
                for i := 0; i < n; i++ {
                        elem := reflect.New(t.Elem())
                        err := d.Decode(elem.Interface())
                        if err != nil {
                                return err
                        }
                        v.Set(reflect.Append(v, elem.Elem()))
                }
                err := d.Decode(&n)
                if err != nil {
                        return err
                }
        }
        return nil
}
