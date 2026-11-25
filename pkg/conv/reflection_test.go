package conv

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type (
	namedBool    bool
	namedString  string
	namedInteger int64
	namedUint    uint64
	namedFloat   float32
	namedComplex complex64
)

func TestNamedNumericTypes(t *testing.T) {
	unit := namedInteger(7)
	pointer := unsafe.Pointer(&unit)
	epoch := time.Unix(0, 7)
	on, off := true, false
	c64, c128 := complex64(7+2i), complex128(7+2i)

	for _, input := range []any{unit, &unit, namedUint(7), namedFloat(7), namedComplex(7 + 2i), uintptr(7), c64, &c64, c128, &c128, epoch, &epoch} {
		t.Run(fmt.Sprintf("%T", input), func(t *testing.T) {
			require.EqualValues(t, 7, Int64(input))
			require.EqualValues(t, 7, Uint64(input))
			require.EqualValues(t, 7, Duration(input))
		})
	}
	for _, input := range []any{unit, &unit, namedUint(7), namedFloat(7), uintptr(7), epoch, &epoch} {
		require.EqualValues(t, 7, Float64(input), "%T", input)
	}
	for _, input := range []any{c64, &c64, c128, &c128, namedComplex(7 + 2i)} {
		require.Zero(t, Float64(input), "%T", input)
	}
	for _, input := range []any{on, &on, namedBool(true)} {
		require.EqualValues(t, 1, Float64(input), "%T", input)
		require.EqualValues(t, 1, Int64(input), "%T", input)
		require.EqualValues(t, 1, Uint64(input), "%T", input)
		require.Zero(t, Duration(input))
	}
	for _, input := range []any{off, &off, namedBool(false), struct{}{}} {
		require.Zero(t, Float64(input), "%T", input)
		require.Zero(t, Int64(input), "%T", input)
		require.Zero(t, Uint64(input), "%T", input)
		require.Zero(t, Duration(input), "%T", input)
	}
	require.Equal(t, int64(uintptr(pointer)), Int64(pointer))
	require.Equal(t, uint64(uintptr(pointer)), Uint64(pointer))
	require.Equal(t, float64(uintptr(pointer)), Float64(pointer))
	require.Equal(t, time.Duration(uintptr(pointer)), Duration(pointer))
}

func TestDurationAndIntegerWireInputs(t *testing.T) {
	wire := []byte{1, 2}
	require.EqualValues(t, 258, Int64(wire))
	require.EqualValues(t, 258, Int64(&wire))
	span := []byte("1.5d2h")
	for _, input := range []any{span, &span, namedString("1.5d2h")} {
		require.Equal(t, 38*time.Hour, Duration(input))
	}
	require.Zero(t, Duration(strings.Repeat("9", 400)+"d"))
}

func TestNamedTypesToBytes(t *testing.T) {
	type small int8
	type medium int16
	type large int32
	type byteValue uint8
	type word uint16
	type doubleWord uint32

	point := uintptr(7)
	wide := binary.BigEndian.AppendUint64(nil, 7)
	tests := []struct {
		input any
		want  []byte
	}{
		{point, wide},
		{&point, wide},
		{namedInteger(7), wide},
		{namedUint(7), wide},
		{small(-2), []byte{0xfe}},
		{medium(258), []byte{1, 2}},
		{large(258), []byte{0, 0, 1, 2}},
		{byteValue(7), []byte{7}},
		{word(258), []byte{1, 2}},
		{doubleWord(258), []byte{0, 0, 1, 2}},
		{namedFloat(7), binary.BigEndian.AppendUint64(nil, math.Float64bits(7))},
		{namedBool(true), []byte{1}},
		{namedString("snow"), []byte("snow")},
		{namedComplex(7 + 2i), nil},
		{(*namedInteger)(nil), nil},
		{struct{ Value int }{7}, []byte(`{"Value":7}`)},
		{make(chan int), nil},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T", tt.input), func(t *testing.T) {
			require.Equal(t, tt.want, Bytes(tt.input))
		})
	}
	number := namedInteger(7)
	require.Equal(t, wide, Bytes(&number))
}

func TestNamedTypesToStringsAndJSON(t *testing.T) {
	c64, c128 := complex64(7+2i), complex128(7+2i)
	number := namedInteger(7)
	tests := []struct {
		input any
		want  string
	}{
		{&number, "7"},
		{namedUint(7), "7"},
		{namedFloat(7), "7"},
		{namedBool(true), "true"},
		{namedString("snow"), "snow"},
		{(*namedString)(nil), ""},
		{&c64, "(7e+00+2e+00i)"},
		{&c128, "(7e+00+2e+00i)"},
		{namedComplex(7 + 2i), "(7e+00+2e+00i)"},
		{struct{ Value int }{7}, `{"Value":7}`},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, String(tt.input), "%T", tt.input)
	}
	ch := make(chan int)
	require.Equal(t, fmt.Sprint(ch), String(ch))
	require.Equal(t, `{"Value":7}`, JSON(namedString(`{"Value":7}`)))
	require.Empty(t, JSON(namedString("not an object")))
	require.Empty(t, JSON(struct{ Value chan int }{ch}))
}

func TestBooleanCoercionAndSliceInputs(t *testing.T) {
	number := 7
	pointer := unsafe.Pointer(&number)
	for _, input := range []any{uintptr(1), pointer, namedUint(1), namedComplex(1 + 2i), [1]int{1}, []int{1}, map[int]int{1: 2}, struct{}{}, func() {}} {
		require.True(t, Bool(input), "%T", input)
	}
	for _, input := range []any{(*namedBool)(nil), unsafe.Pointer(nil), map[int]int{}, (<-chan int)(nil)} {
		require.False(t, Bool(input), "%T", input)
	}

	typed := []int64{7}
	require.Equal(t, typed, Int64s(typed))
	require.Equal(t, typed, Int64s(&typed))
	require.Equal(t, typed, Int64s([1]int{7}))
	require.Nil(t, Int64s(nil))
	require.Nil(t, Int64s(7))
	require.Equal(t, []any{7}, Interfaces(&typed))
}
