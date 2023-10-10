package skiplist

type Iterator struct {
	current *node
}

func (sl *SkipList) Iterator() *Iterator {
	return &Iterator{sl.head.tower[0]}
}

func (i *Iterator) HasNext() bool {
	return i.current.tower[0] != nil
}

func (i *Iterator) Next() ([]byte, []byte) {
	i.current = i.current.tower[0]

	if i.current == nil {
		return nil, nil
	}
	return i.current.key, i.current.val
}
