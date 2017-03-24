package rocksdb

//A generic stack implementation
type Stack struct {
	top *Element
	size int
}

type Element struct {
	value interface{}
	next *Element
}

// Return the stack's length
func (s *Stack) Len() int {
	return s.size
}

func (s *Stack) Push(value interface{}) {
	s.top = &Element{value, s.top}
	s.size++
}

// Remove the top element from the stack and return it's value
// If the stack is empty, return nil
func (s *Stack) Pop() (value interface{}) {
	if s.size <= 0 {
		return nil
	}

	value, s.top = s.top.value, s.top.next
	s.size--
	return value
}
