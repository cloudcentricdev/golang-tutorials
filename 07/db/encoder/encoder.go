package encoder

type OpKind uint8

const (
	OpKindDelete OpKind = iota
	OpKindSet
)

type Encoder struct{}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Encode(opKind OpKind, val []byte) []byte {
	n := len(val)
	buf := make([]byte, n+1)
	buf[0] = byte(opKind)
	copy(buf[1:], val)
	return buf
}

func (e *Encoder) Parse(val []byte) *EncodedValue {
	n := len(val)
	buf := make([]byte, n-1)
	opKind := val[0]
	copy(buf, val[1:])
	return &EncodedValue{val: buf, opKind: OpKind(opKind)}
}

type EncodedValue struct {
	val    []byte
	opKind OpKind
}

func (ev *EncodedValue) Value() []byte {
	return ev.val
}

func (ev *EncodedValue) IsTombstone() bool {
	return ev.opKind == OpKindDelete
}
