package tasks

import (
	"maps"
	"slices"
	"strconv"

	"go.temporal.io/server/common/headers"
)

type (
	Priority int
)

const (
	numBitsPerLevel = 3
)

const (
	highPriorityClass Priority = iota << numBitsPerLevel
	mediumPriorityClass
	lowPriorityClass
)

const (
	highPrioritySubclass Priority = iota
	mediumPrioritySubclass
	lowPrioritySubclass
)

var (
	PriorityHigh        = getPriority(highPriorityClass, mediumPrioritySubclass)
	PriorityLow         = getPriority(highPriorityClass, lowPrioritySubclass)
	PriorityPreemptable = getPriority(lowPriorityClass, mediumPrioritySubclass)
)

var (
	PriorityName = map[Priority]string{
		PriorityHigh:        "high",
		PriorityLow:         "low",
		PriorityPreemptable: "preemptable",
	}

	PriorityValue = map[string]Priority{
		"high":        PriorityHigh,
		"low":         PriorityLow,
		"preemptable": PriorityPreemptable,
	}

	CallerTypeToPriority = map[string]Priority{
		headers.CallerTypeBackgroundHigh: PriorityHigh,
		headers.CallerTypeBackgroundLow:  PriorityLow,
		headers.CallerTypePreemptable:    PriorityPreemptable,
	}

	PriorityToCallerType = map[Priority]string{
		PriorityHigh:        headers.CallerTypeBackgroundHigh,
		PriorityLow:         headers.CallerTypeBackgroundLow,
		PriorityPreemptable: headers.CallerTypePreemptable,
	}

	// PriorityOrder lists every named priority, most urgent first. A lower Priority is more
	// urgent, so this is PriorityName's keys ascending; adding a priority there orders it
	// here too.
	PriorityOrder = slices.Sorted(maps.Keys(PriorityName))
)

func (p Priority) String() string {
	s, ok := PriorityName[p]
	if ok {
		return s
	}
	return strconv.Itoa(int(p))
}

func (p Priority) CallerType() string {
	s, ok := PriorityToCallerType[p]
	if ok {
		return s
	}
	return headers.CallerTypePreemptable
}

func getPriority(
	class, subClass Priority,
) Priority {
	return class | subClass
}
