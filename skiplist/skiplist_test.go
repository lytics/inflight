package skiplist

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
	//"github.com/pkg/profile"
)

const (
	maxN = 1000000
)

type Element uint64

func (e Element) ExtractID() uint64 {
	return uint64(e)
}
func (e Element) Element() interface{} {
	return e
}
func (e Element) String() string {
	return fmt.Sprintf("%03d", e)
}

type ComplexElement struct {
	E int
	S string
}

func (e ComplexElement) ExtractID() uint64 {
	return uint64(e.E)
}
func (e ComplexElement) Element() interface{} {
	return e.E
}
func (e ComplexElement) String() string {
	return fmt.Sprintf("%03d", e.E)
}

// timeTrack will print out the number of nanoseconds since the start time divided by n
// Useful for printing out how long each iteration took in a benchmark
func timeTrack(start time.Time, n int, name string) {
	loopNS := time.Since(start).Nanoseconds() / int64(n)
	fmt.Printf("%s: %d\n", name, loopNS)
}

func TestInsertAndFind(t *testing.T) {
	var list SkipList

	var listPointer *SkipList
	listPointer.Insert(Element(0))
	if _, ok := listPointer.Find(Element(0)); ok {
		t.Fail()
	}

	list = New()

	if _, ok := list.Find(Element(0)); ok {
		t.Fail()
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	// Test at the beginning of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(maxN - i))
	}
	for i := 0; i < maxN; i++ {
		if _, ok := list.Find(Element(maxN - i)); !ok {
			t.Fail()
		}
	}

	list = New()
	// Test at the end of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}
	for i := 0; i < maxN; i++ {
		if _, ok := list.Find(Element(i)); !ok {
			t.Fail()
		}
	}

	list = New()
	// Test at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		list.Insert(Element(e))
	}
	for _, e := range rList {
		if _, ok := list.Find(Element(e)); !ok {
			t.Fail()
		}
	}

}

func TestDelete(t *testing.T) {

	var list SkipList

	// Delete on empty list
	list.Delete(Element(0))

	list = New()

	list.Delete(Element(0))
	if !list.IsEmpty() {
		t.Fail()
	}

	// Delete elements at the beginning of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(i))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New()
	// Delete elements at the end of the list.
	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}
	for i := 0; i < maxN; i++ {
		list.Delete(Element(maxN - i - 1))
	}
	if !list.IsEmpty() {
		t.Fail()
	}

	list = New()
	// Delete elements at random positions in the list.
	rList := rand.Perm(maxN)
	for _, e := range rList {
		list.Insert(Element(e))
	}
	for _, e := range rList {
		list.Delete(Element(e))
	}
	if !list.IsEmpty() {
		t.Fail()
	}
}

func TestFindGreaterOrEqual(t *testing.T) {
	maxNumber := 1000

	var list SkipList
	var listPointer *SkipList

	// Test on empty list.
	if _, ok := listPointer.FindGreaterOrEqual(Element(0)); ok {
		t.Fail()
	}

	list = New()

	largestN := int64(math.MinInt64)
	for i := 0; i < maxN; i++ {
		n := int64(rand.Int() * maxNumber)
		if largestN < n {
			largestN = n
		}
		list.Insert(Element(n))
	}

	first := list.GetSmallestNode().GetValue().(Element)

	// Find the very first element. This is a special case in the implementation that needs testing!
	if v, ok := list.FindGreaterOrEqual(Element(first - 2)); ok {
		element := v.GetValue().(Element)
		if first != element {
			t.Logf("We found an element different to the first one: expected:%v found:%v", first, element)
			t.FailNow()
		}
	} else {
		t.Logf("No element found for first-2.")
		t.FailNow()
	}

	for i := 0; i < maxN; i++ {
		findV := Element(rand.Int() * maxNumber)
		if v, ok := list.FindGreaterOrEqual(Element(findV)); ok {
			// if f is v should be bigger than the element before
			lastV := list.Prev(v).GetValue().(Element)
			thisV := v.GetValue().(Element)
			isFirst := first == thisV
			if !isFirst && lastV >= findV {
				t.Logf("\nlastV: %v\nfindV: %v\n\n", lastV, findV)
				t.Fail()
			}
		} else {
			lastV := list.GetLargestNode().GetValue().(Element)
			// It is OK, to fail, as long as f is bigger than the last element.
			if findV <= lastV {
				t.Logf("\nlastV: %v\nfindV: %v\n\n", lastV, findV)
				t.Fail()
			}
		}
	}
}

func TestPrev(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	smallest := list.GetSmallestNode()
	largest := list.GetLargestNode()

	lastNode := largest
	node := lastNode
	for node != smallest {
		node = list.Prev(node)
		// Must always be incrementing here!
		if node.value.(Element) >= lastNode.value.(Element) {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		if list.Prev(list.Next(node)) != node {
			t.Fail()
		}
		lastNode = node
	}

	if list.Prev(smallest) != largest {
		t.Fail()
	}
}

func TestNext(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	smallest := list.GetSmallestNode()
	largest := list.GetLargestNode()

	lastNode := smallest
	node := lastNode
	for node != largest {
		node = list.Next(node)
		// Must always be incrementing here!
		if node.value.(Element) <= lastNode.value.(Element) {
			t.Fail()
		}
		// Next.Prev must always point to itself!
		if list.Next(list.Prev(node)) != node {
			t.Fail()
		}
		lastNode = node
	}

	if list.Next(largest) != smallest {
		t.Fail()
	}
}

func TestChangeValue(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(ComplexElement{i, "value"})
	}

	for i := 0; i < maxN; i++ {
		// The key only looks at the int so the string doesn't matter here!
		f1, ok := list.Find(ComplexElement{i, ""})
		if !ok {
			t.Fail()
		}
		ok = list.ChangeValue(f1, ComplexElement{i, "different value"})
		if !ok {
			t.Fail()
		}
		f2, ok := list.Find(ComplexElement{i, ""})
		if !ok {
			t.Fail()
		}
		if f2.GetValue().(ComplexElement).S != "different value" {
			t.Fail()
		}
		if ok = list.ChangeValue(f2, ComplexElement{i + 5, "different key"}); ok {
			t.Fail()
		}
	}
}

func TestGetNodeCount(t *testing.T) {
	list := New()

	for i := 0; i < maxN; i++ {
		list.Insert(Element(i))
	}

	if list.GetNodeCount() != maxN {
		t.Fail()
	}
}

func TestString(t *testing.T) {
	list := NewSeed(1531889620180049576)

	for i := 0; i < 20; i++ {
		list.Insert(Element(i))
	}

	testString := ` --> [000]     -> [002] -> [009] -> [010]
000: [---|001]
001: [000|002]
002: [001|003] -> [004]
003: [002|004]
004: [003|005] -> [005]
005: [004|006] -> [009]
006: [005|007]
007: [006|008]
008: [007|009]
009: [008|010] -> [010] -> [010]
010: [009|011] -> [012] -> [---] -> [---]
011: [010|012]
012: [011|013] -> [013]
013: [012|014] -> [---]
014: [013|015]
015: [014|016]
016: [015|017]
017: [016|018]
018: [017|019]
019: [018|---]
 --> [019]     -> [013] -> [010] -> [010]
`

	if list.String() != testString {
		t.Fail()
	}
}

func TestInfiniteLoop(t *testing.T) {
	list := New()
	list.Insert(Element(1))

	if _, ok := list.Find(Element(2)); ok {
		t.Fail()
	}

	if _, ok := list.FindGreaterOrEqual(Element(2)); ok {
		t.Fail()
	}
}

func BenchmarkMapInsert(b *testing.B) {
	m := map[Element]Element{}
	for n := 0; n < b.N*1000000; n++ {
		e := Element(n)
		m[Element(n)] = e
	}
}

func BenchmarkSkipListInsert(b *testing.B) {
	list := New()
	for n := 0; n < b.N*1000000; n++ {
		e := Element(n)
		list.Insert(e)
	}
}

func BenchmarkMapFind(b *testing.B) {
	m := map[Element]Element{}
	max := b.N * 1000000
	for n := 0; n < max; n++ {
		e := Element(n)
		m[Element(n)] = e
	}

	b.ResetTimer()
	for n := 0; n < max; n++ {
		if _, ok := m[Element(n)]; !ok {
			b.Logf("failed to find %v", n)
			b.FailNow()
		}
	}
}

func BenchmarkSkipListFind(b *testing.B) {
	list := New()
	max := b.N * 1000000
	for n := 0; n < max; n++ {
		e := Element(n)
		list.Insert(e)
	}

	b.ResetTimer()
	for n := 0; n < max; n++ {
		if _, ok := list.Find(Element(n)); !ok {
			b.Logf("failed to find %v", n)
			b.FailNow()
		}
	}
}
