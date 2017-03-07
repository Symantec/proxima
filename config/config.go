package config

import (
	"sort"
)

type byDurationDesc []Influx

func (b byDurationDesc) Len() int {
	return len(b)
}

func (b byDurationDesc) Less(i, j int) bool {
	return b[i].Duration > b[j].Duration
}

func (b byDurationDesc) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func orderInfluxes(influx []Influx) {
	sort.Stable(byDurationDesc(influx))
}
