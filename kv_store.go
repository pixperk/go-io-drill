package main

import (
	"errors"
	"sync"
	"time"
)

type key struct {
	name string
	ttl  time.Duration
}

type value struct {
	data      string
	timestamp time.Time
}

type wal struct {
	filename string
	//why not using RWMutex here?
	//because we want to allow only one writer at a time
	//but multiple readers can read concurrently
	//so a simple Mutex is sufficient
	wal_lock sync.Mutex
}

type key_val_pair_map map[key]value

type store struct {
	data key_val_pair_map
	lock sync.RWMutex
	wal  *wal
}

func new_wal(filename string) *wal {
	return &wal{
		filename: filename,
		wal_lock: sync.Mutex{},
	}
}

func new_store(wal_filename string) *store {
	return &store{
		data: make(key_val_pair_map),
		lock: sync.RWMutex{},
		wal:  new_wal(wal_filename),
	}
}

func (s *store) Get(k key) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	val, exists := s.data[k]
	if !exists {
		return "", false
	}

	//check if key has expired
	if k.ttl > 0 && time.Since(val.timestamp) > k.ttl {
		//key has expired
		//delete it from store
		s.lock.RUnlock() //unlock read lock before acquiring write lock
		s.lock.Lock()
		delete(s.data, k)
		s.lock.Unlock()
		s.lock.RLock() //reacquire read lock
		return "", false
	}
	return val.data, true
}

func (s *store) Set(k key, v string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data[k] = value{
		data:      v,
		timestamp: time.Now(),
	}
	return nil
}

func (s *store) Delete(k key) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.data, k)
}

func (s *store) Expire(k key, ttl time.Duration) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	val, exists := s.data[k]
	if !exists {
		return errors.New("the key does not exist")
	}

	val.timestamp = time.Now().Add(-k.ttl + ttl)
	s.data[k] = val
	return nil
}

func (s *store) Exists(k key) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, exists := s.data[k]
	return exists
}

func (s *store) Ttl(k key) (time.Time, time.Duration, time.Time, error) { //returns current time, ttl duration, expiry time, error
	s.lock.RLock()
	defer s.lock.RUnlock()

	val, exists := s.data[k]
	if !exists {
		return time.Time{}, 0, time.Time{}, errors.New("the key does not exist")
	}

	if k.ttl == 0 {
		return time.Now(), 0, time.Time{}, nil //no expiry
	}

	expiry_time := val.timestamp.Add(k.ttl)
	remaining_ttl := expiry_time.Sub(time.Now())
	return time.Now(), remaining_ttl, expiry_time, nil
}
