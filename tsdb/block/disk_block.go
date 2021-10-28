package block

import "github.com/vmihailenco/msgpack/v5"

type DiskBlock struct {
	MinTimestamp uint64
	MaxTimestamp uint64
	Events       map[string]eventBlock
}

type eventMetaData struct {
	MinTimestamp  uint64
	MaxTimestamp  uint64
	NumDataPoints uint64
	Offset        uint64
}

type dataBlock struct {
	data []byte
}

func (b *Block)

func (b *Block) Marshal() ([]byte, error) {
	return msgpack.Marshal(&b)
}

func (b *Block) Unmarshal(bytes []byte) error {
	return msgpack.Unmarshal(bytes, &b)
}
