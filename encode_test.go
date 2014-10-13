package avro

import (
        "bytes"
        "testing"
)

func testEncodeSupport(t *testing.T, x interface{}) {
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)
        err := enc.Encode(x)
        if err != nil {
                t.Error(t)
        }
}

func TestEncodeSupport(t *testing.T) {
        // null
        testEncodeSupport(t, Null(0))

        // bool
        testEncodeSupport(t, true)

        // int long
        testEncodeSupport(t, 1)
        testEncodeSupport(t, int(1))
        testEncodeSupport(t, int8(1))
        testEncodeSupport(t, int16(1))
        testEncodeSupport(t, int32(1))
        testEncodeSupport(t, int64(1))
        testEncodeSupport(t, uint(1))
        testEncodeSupport(t, uint8(1))
        testEncodeSupport(t, uint32(1))
        testEncodeSupport(t, uint64(1))

        // float double
        testEncodeSupport(t, float32(1))
        testEncodeSupport(t, float64(1))

        // bytes
        testEncodeSupport(t, []byte{})

        // string
        testEncodeSupport(t, "")

        // record
        testEncodeSupport(t, struct{}{})

        // enum
        testEncodeSupport(t, 1)

        // array
        testEncodeSupport(t, []byte{1, 2, 3})

        // map
        testEncodeSupport(t, map[string]string{})

        // Unions
        testEncodeSupport(t, Union{0, []interface{}{0}})

        // fixed
        testEncodeSupport(t, [3]byte{1, 2, 3})
}

func TestEncodeNil(t *testing.T) {
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)
        enc.Encode(nil)
        if buf.Len() != 0 {
                t.Error("should 0")
        }
        var null Null
        if err := enc.Encode(null); err != nil {
                t.Error(err)
        }

        if buf.Len() != 0 {
                t.Error("should 0")
        }
}

func TestEncodeInt(t *testing.T) {
        m := map[int]byte{
                0:  0,
                -1: 1,
                1:  2,
                -2: 3,
                2:  4,
                -3: 5,
                3:  6,
        }
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)
        for k, v := range m {
                buf.Reset()
                if err := enc.Encode(k); err != nil {
                        t.Error(err)
                        continue
                }
                if buf.Bytes()[0] != v {
                        t.Error(buf.Bytes()[0])
                }
        }
}

func TestEncodeBool(t *testing.T) {
        m := map[bool]byte{
                true:  1,
                false: 0,
        }
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for k, v := range m {
                buf.Reset()
                if err := enc.Encode(k); err != nil {
                        t.Error(err)
                        continue
                }
                if buf.Bytes()[0] != v {
                        t.Error(buf.Bytes()[0])
                }
        }
}

func TestEncodeString(t *testing.T) {
        m := map[string][]byte{
                "foo": []byte{0x06, 0x66, 0x6f, 0x6f},
        }
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for k, v := range m {
                buf.Reset()
                if err := enc.Encode(k); err != nil {
                        t.Error(err)
                        continue
                }
                if !bytes.Equal(buf.Bytes(), v) {
                        t.Error(buf.Bytes())
                }
        }
}

func TestEncodeUnion(t *testing.T) {
        u := Union{
                Elem: []interface{}{
                        Null(0),
                        int(2),
                        "foo",
                },
        }
        m := map[int][]byte{
                0: []byte{0},
                1: []byte{2, 4},
                2: []byte{0x04, 0x06, 0x66, 0x6f, 0x6f},
        }

        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for k, v := range m {
                buf.Reset()
                u.Idx = k
                err := enc.Encode(u)
                if err != nil {
                        t.Error(err)
                }
                if !bytes.Equal(buf.Bytes(), v) {
                        t.Error(buf.Bytes())
                }
        }
}

func TestEncodeFixed(t *testing.T) {
        m := map[string][]byte{
                "foo": []byte{0x66, 0x6f, 0x6f},
                "abc": []byte{0x61, 0x62, 0x63},
        }
        fix := [3]byte{}
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for k, v := range m {
                buf.Reset()
                copy(fix[:], k)
                if err := enc.Encode(fix); err != nil {
                        t.Error(err)
                        continue
                }
                if !bytes.Equal(buf.Bytes(), v) {
                        t.Error(buf.Bytes())
                }
        }
}

func TestEncodeArray(t *testing.T) {
        m := [][]int{
                []int{0, 1, 2, 3},
        }
        expect := [][]byte{
                []byte{8, // length
                        0, 2, 4, 6,
                        0, // end
                },
        }
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for i, v := range m {
                buf.Reset()
                if err := enc.Encode(v); err != nil {
                        t.Error(err)
                        continue
                }
                if !bytes.Equal(buf.Bytes(), expect[i]) {
                        t.Error(buf.Bytes())
                }
        }
}

func TestEncodeMap(t *testing.T) {
        m := []map[string]string{
                map[string]string{"abcd": "foo"},
        }
        expect := [][]byte{
                []byte{0x02, // 1 key-value
                        0x08,                   // key length
                        0x61, 0x62, 0x63, 0x64, // key
                        0x06,             // value length
                        0x66, 0x6f, 0x6f, // value
                        0x00, // end
                },
        }

        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        for i, v := range m {
                buf.Reset()
                if err := enc.Encode(v); err != nil {
                        t.Error(err)
                        continue
                }

                if !bytes.Equal(buf.Bytes(), expect[i]) {
                        t.Error(buf.Bytes())
                }
        }

}

func TestEncodeStruct(t *testing.T) {
        m := struct {
                Int    int
                Nil    Null
                skip   int
                Fixed  [3]byte
                String string
                Map    map[string]string
                Un     Union
        }{
                1,
                Null(0),
                2,
                [3]byte{1, 2, 3},
                "abc",
                map[string]string{"a": "b"},
                Union{1, []interface{}{Null(0), "a"}},
        }
        expect := []byte{
                2, // int(1)
                // Nil(skip)
                // skip(skip)
                1, 2, 3, // fixed
                6, 0x61, 0x62, 0x63, // string
                2, 2, 0x61, 2, 0x62, 0, // map
                2, 2, 0x61, // union
        }
        buf := new(bytes.Buffer)
        enc := NewEncoder(buf)

        if err := enc.Encode(m); err != nil {
                t.Error(err)
        }
        if !bytes.Equal(buf.Bytes(), expect) {
                t.Error(buf.Bytes())
        }
}
