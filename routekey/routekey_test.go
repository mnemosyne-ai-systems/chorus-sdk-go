package routekey

import "testing"

func TestHashStableAndSpread(t *testing.T) {
	if Hash("abc") != Hash("abc") {
		t.Fatal("hash must be deterministic")
	}
	if Hash("abc") == Hash("abd") {
		t.Fatal("adjacent inputs should not collide")
	}

	var counts [4]int
	for i := 0; i < 2000; i++ {
		sid := string(rune('a'+i%26)) + "-" + itoa(i)
		counts[Hash(sid)>>62]++
	}
	for i, count := range counts {
		if count == 0 || count > 800 {
			t.Fatalf("bucket %d = %d, distribution broken", i, count)
		}
	}
}

func TestContainsRangeSemantics(t *testing.T) {
	const full = 0
	for _, h := range []uint64{0, 1, 1 << 32, ^uint64(0)} {
		if !Contains(0, full, h) {
			t.Fatalf("full space must contain %d", h)
		}
	}

	if Contains(100, 200, 99) || !Contains(100, 200, 100) ||
		!Contains(100, 200, 199) || Contains(100, 200, 200) {
		t.Fatal("[100,200) boundary wrong")
	}

	lo := ^uint64(0) - 5
	if !Contains(lo, 0, ^uint64(0)) || Contains(lo, 0, ^uint64(0)-6) {
		t.Fatal("tail [max-5, 2^64) wrong")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
