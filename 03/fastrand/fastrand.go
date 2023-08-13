package fastrand

import _ "unsafe" // required by go:linkname

//go:linkname Uint32 runtime.fastrand
func Uint32() uint32
