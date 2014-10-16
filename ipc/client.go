package ipc

import (
        "avro"
        "io"
        "net"
        "net/rpc"
        "sync"
)

type clientCodec struct {
        rwc       io.ReadWriteCloser
        dec       *avro.Decoder
        enc       *avro.Encoder
        fout      *Frame
        fin       *Frame
        proto     []byte
        handShake bool
        mutex     sync.Mutex
}

type HandShakeError int

func (e HandShakeError) Error() string {
        return "handshake error"
}

func NewClientCodec(rwc io.ReadWriteCloser, proto []byte) *clientCodec {
        var fin, fout Frame
        dec := avro.NewDecoder(&fin)
        enc := avro.NewEncoder(&fout)
        return &clientCodec{
                rwc:   rwc,
                dec:   dec,
                enc:   enc,
                fout:  &fout,
                fin:   &fin,
                proto: proto,
        }
}

func (c *clientCodec) writeHandShake() error {
        req := NewHandShakeRequest(c.proto)
        return c.enc.Encode(req)
}

func (c *clientCodec) readHandShake() error {
        rep := NewHandShakeResponse(NONE, c.proto)
        err := c.dec.Decode(rep)
        if err != nil {
                return err
        }
        if rep.Match == NONE {
                return HandShakeError(rep.Match)
        }
        return nil
}

func (c *clientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
        c.mutex.Lock()
        defer c.mutex.Unlock()
        if !c.handShake {
                err := c.writeHandShake()
                if err != nil {
                        return err
                }
        }
        req := Request{
                Method:  r.ServiceMethod,
                Payload: param,
        }
        err := c.enc.Encode(&req)
        if err != nil {
                return err
        }
        c.fout.Xid = int32(r.Seq)
        return c.fout.Encode(c.rwc)
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) error {
        err := c.fin.Decode(c.rwc)
        if err != nil {
                return err
        }
        if !c.handShake {
                err := c.readHandShake()
                _, ok := err.(HandShakeError)
                if err != nil && !ok {
                        return err
                }
                c.handShake = true
        }
        r.Seq = uint64(c.fin.Xid)
        return nil
}

func (c *clientCodec) ReadResponseBody(x interface{}) error {
        u, ok := x.(*avro.Union)
        if !ok {
                panic("response must be *avro.Union")
        }
        rep := Response{
                Meta:  make(map[string]string),
                Error: false,
        }
        err := c.dec.Decode(&rep)
        if err != nil {
                return err
        }
        if !rep.Error {
                u.Idx = 0
                err = c.dec.Decode(u.Elem[0])
        } else {
                u.Idx = 1
                err = c.dec.Decode(u.Elem[1])
        }
        return err
}

func (c *clientCodec) Close() error {
        return c.rwc.Close()
}

func Dial(addr string, proto []byte) (*rpc.Client, error) {
        conn, err := net.Dial("tcp", addr)
        if err != nil {
                return nil, err
        }
        return rpc.NewClientWithCodec(NewClientCodec(conn, proto)), nil
}
