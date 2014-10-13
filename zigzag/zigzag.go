package zigzag

func Encode(i int64) uint64 {
        return uint64(i<<1 ^ i>>63)
}

func Decode(u int64) int64 {
        return int64(u>>1) ^ -int64(u&1)
}
