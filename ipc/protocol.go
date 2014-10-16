package ipc

import (
        "avro"
        "bytes"
        "crypto/md5"
        "encoding/binary"
        "io"
)

// a frame contains xid(4) + blkSize(4) + blocks
// each block contains header(4) + body
type Frame struct {
        Xid int32
        bytes.Buffer
}

func (f *Frame) Encode(w io.Writer) error {

        t := struct {
                Xid       int32
                BlkSize   int32
                FrameSize int32
        }{f.Xid, 1, int32(f.Len())}
        err := binary.Write(w, binary.BigEndian, &t)
        if err != nil {
                return err
        }
        _, err = f.WriteTo(w)
        return err
}

func (f *Frame) Decode(r io.Reader) error {
        t := struct {
                Xid     int32
                BlkSize int32
        }{}
        err := binary.Read(r, binary.BigEndian, &t)
        if err != nil {
                return err
        }
        for i := 0; int32(i) < t.BlkSize; i++ {
                var size int32
                err = binary.Read(r, binary.BigEndian, &size)
                if err != nil {
                        return err
                }
                b := make([]byte, size)
                _, err = io.ReadFull(r, b)
                if err != nil {
                        return err
                }
                f.Write(b)
        }
        f.Xid = t.Xid
        return nil
}

type HandShakeRequest struct {
        ClientHash     [16]byte
        ClientProtocol avro.Union
        ServerHash     [16]byte
        Meta           avro.Union
}

func NewHandShakeRequest(proto []byte) *HandShakeRequest {
        m := md5.Sum(proto)
        return &HandShakeRequest{
                m,
                avro.MakeUnion(0, new(avro.Null), new(map[string]string)),
                m,
                avro.MakeUnion(0, new(avro.Null), new(map[string]string)),
        }
}

const (
        BOTH   = 0
        CLIENT = 1
        NONE   = 2
)

type HandShakeResponse struct {
        Match          int
        ServerProtocol avro.Union
        ServerHash     avro.Union
        Meta           avro.Union
}

func NewHandShakeResponse(match int, proto []byte) *HandShakeResponse {
        m := md5.Sum(proto)
        return &HandShakeResponse{
                match,
                avro.MakeUnion(0, new(avro.Null), new(string)),
                avro.MakeUnion(0, new(avro.Null), &m),
                avro.MakeUnion(0, new(avro.Null), new(map[string]string)),
        }
}

type Request struct {
        Meta    map[string]string
        Method  string
        Payload interface{}
}

type Response struct {
        Meta  map[string]string
        Error bool
}
