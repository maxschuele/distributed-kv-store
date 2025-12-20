package sharding

import (
	"errors"
	"strconv"
)

type Hasher struct {
	ring *Ring
}

func NewConsistentHash() *Hasher {
	return &Hasher{
		ring: NewRing(nil),
	}
}

func (h *Hasher) AddGroup(groupID int) {
	//convert groupID into string and store it in the ring
	h.ring.Add(strconv.Itoa(groupID))
}

func (h *Hasher) RemoveGroup(groupID int) {
	//convert groupID into string and delete it from the ring
	h.ring.Remove(strconv.Itoa(groupID))
}

func (h *Hasher) KeyToGroup(key string) (int, error) {
	groupStr := h.ring.Get(key)

	if groupStr == "" {
		return 0, errors.New("Ring is Empty")
	}

	// convert string back to int
	id, err := strconv.Atoi(groupStr)
	if err != nil {
		return 0, err
	}
	return id, nil
}
