package hidera

type Aggregate struct {
	Value int
	Count int
	Round int
}

func (a Aggregate) Aggregate(other []Aggregate) Aggregate {
	for _, o := range other {
		a.Value += o.Value
		a.Count += o.Count
	}
	return a
}