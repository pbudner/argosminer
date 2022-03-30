package encoding

// Codec (un)marshales values.
type Codec interface {
	// Marshal encodes a value to a slice of bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a value.
	Unmarshal(data []byte, v interface{}) error
}

var (
	// Gob uses the standard GobCodec.
	Gob = GobCodec{}
)
