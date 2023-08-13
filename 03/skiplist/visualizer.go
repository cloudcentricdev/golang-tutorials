package skiplist

import "fmt"

const (
	ArrowShaft  = "-"
	ArrowHead   = ">"
	Arrow       = ArrowShaft + ArrowHead
	LowestLevel = 0 // index corresponding to the lowest level in SkipList
	Padding     = 2 // number of whitespaces surrounding displayed key
)

type visualizer struct {
	sl *SkipList
}

func (v *visualizer) visualize() string {
	var output string

	lowestLevel := v.extractLowestLevel()

	for level := v.sl.height - 1; level >= 0; level-- {
		output += fmt.Sprintf("L%02d ", level)
		for i, next := 0, v.sl.head.tower[level]; next != nil; i, next = i+1, next.tower[level] {
			var key string
			for key = string(next.key); lowestLevel[i] != key; i++ {
				output += v.paddedArrowShaft(len(lowestLevel[i]))
			}
			output += fmt.Sprintf("%v %v ", Arrow, key)
		}
		output += "\n"
	}

	return output
}

func (v *visualizer) extractLowestLevel() []string {
	var lowestLevel []string
	for next := v.sl.head.tower[LowestLevel]; next != nil; next = next.tower[LowestLevel] {
		lowestLevel = append(lowestLevel, string(next.key))
	}
	return lowestLevel
}

func (v *visualizer) paddedArrowShaft(keyLen int) string {
	var str string
	for i := 0; i < keyLen+len(Arrow)+Padding; i++ {
		str += ArrowShaft
	}
	return str
}
