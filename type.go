package avro

type Union struct {
        Idx  int
        Elem []interface{}
}

func MakeUnion(idx int, elem ...interface{}) Union {
        return Union{
                Idx:  idx,
                Elem: elem,
        }
}

type Null int
