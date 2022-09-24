package list

import (
	//"errors"
	//"fmt"
	"io"
	"strings"

	repl "github.com/brown-csci1270/db/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	l := List{}
	return &l
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	new_link := Link{}
	new_link.value = value
	new_link.list = list
	if list.head == nil {
		list.head = &new_link
		list.tail = &new_link
	} else {
		cur_head := list.head
		cur_head.prev = &new_link
		list.head = &new_link
		new_link.next = cur_head
	}
	return &new_link
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	new_link := Link{}
	new_link.value = value
	new_link.list = list
	if list.head == nil {
		list.head = &new_link
		list.tail = &new_link
	} else {
		cur_tail := list.tail
		cur_tail.next = &new_link
		list.tail = &new_link
		new_link.prev = cur_tail
	}
	return &new_link
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	for cur := list.head; cur != nil; cur = cur.next {
		if f(cur) == true {
			return cur
		}
	}
	return nil
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	for cur := list.head; cur != nil; cur = cur.next {
		f(cur)
	}
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	list := link.list
	if list.head == link {
		list.head = link.next
	}
	if list.tail == link {
		list.tail = link.prev
	}
	if link.next != nil {
		link.next.prev = link.prev
	}
	if link.prev != nil {
		link.prev.next = link.next
	}
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	r := repl.NewRepl()

	list_print_func := func(cmd string, r *repl.REPLConfig) error{
		cur := list.PeekHead()
		for ; cur != nil; cur = cur.next {
			io.WriteString(r.GetWriter(), cur.value.(string))
		}
		return nil
	}
	r.AddCommand("list_print", list_print_func, "Prints out all of the elements in the list in order, separated by commas (e.g. \"0, 1, 2\")")

	list_push_head_func := func(cmd string, r *repl.REPLConfig) error{
		elements := strings.Split(cmd, " ")
		elt := elements[1]
		list.PushHead(elt)
		return nil
	}
	r.AddCommand("list_push_head", list_push_head_func, "Inserts the given element to the List as a string.")

	list_push_tail_func := func(cmd string, r *repl.REPLConfig) error{
		elements := strings.Split(cmd, " ")
		elt := elements[1]
		list.PushTail(elt)
		return nil
	}
	r.AddCommand("list_push_tail", list_push_tail_func, "Inserts the given element to the end of the List as a string.")

	list_remove_func := func(cmd string, r *repl.REPLConfig) error{
		elements := strings.Split(cmd, " ")
		elt := elements[1]
		cur := list.PeekHead()
		for ; cur != nil; cur = cur.next {
			if cur.value == elt {
				cur.PopSelf()
				break
			}
		}
		return nil
	}
	r.AddCommand("list_remove", list_remove_func, "Removes the given element from the list.")

	list_contains_func := func(cmd string, r *repl.REPLConfig) error{
		elements := strings.Split(cmd, " ")
		elt := elements[1]
		f := func(l *Link) bool {
			if l.value == elt {
				return true
			}
			return false
		}

		if list.Find(f) != nil{
			io.WriteString(r.GetWriter(), "found!")
		} else {
			io.WriteString(r.GetWriter(), "not found")
		}
		return nil
	}
	r.AddCommand("list_contains", list_contains_func, "Prints \"found!\" if the element is in the list, prints \"not found\" otherwise.\n")

	return r
}