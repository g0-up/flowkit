package testhooks

import (
	"slices"
	"testing"
)

func TestReachWithoutPointDoesNothing(t *testing.T) {
	if Point != nil {
		t.Fatal("Point is set outside a test")
	}
	Reach(PointClaimed)
}

func TestReachCallsPoint(t *testing.T) {
	var got []string
	Point = func(name string) { got = append(got, name) }
	t.Cleanup(func() { Point = nil })
	Reach(PointClaimed)
	Reach(PointRouted)
	if want := []string{PointClaimed, PointRouted}; !slices.Equal(got, want) {
		t.Fatalf("Point got %v, want %v", got, want)
	}
}
