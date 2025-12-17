package main

import (
	"bufio"
	"errors"
	"log"
	"os"
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

type operation_type int

const (
	SET operation_type = iota
	DELETE
	EXPIRE
)

func (w *wal) log_op(key key, op operation_type, value string, ttl time.Duration) error {
	w.wal_lock.Lock()
	defer w.wal_lock.Unlock()

	//open file in append mode
	//create if not exists
	fd, err := os.OpenFile(w.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	var log_entry string
	switch op {
	case SET:
		log_entry = "SET " + key.name + " " + value + "\n"
	case DELETE:
		log_entry = "DELETE " + key.name + "\n"
	case EXPIRE:
		log_entry = "EXPIRE " + key.name + " " + ttl.String() + "\n"
	default:
		return errors.New("unknown operation type")
	}

	writer := bufio.NewWriter(fd)

	n := 0
	for n < len(log_entry) {
		nn, err := writer.WriteString(log_entry[n:])
		if err != nil {
			return err
		}
		n += nn
	}

	err = writer.Flush()

	if err != nil {
		return err
	}

	err = fd.Sync()
	if err != nil {
		return err
	}

	log.Printf("\nlogged operation to WAL: %s\n", log_entry)
	return nil
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
		return "", false
	}
	return val.data, true
}

func (s *store) Set(k key, v string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.wal.log_op(k, SET, v, 0); err != nil {
		return err
	}

	s.data[k] = value{
		data:      v,
		timestamp: time.Now(),
	}
	return nil
}

func (s *store) Delete(k key) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.wal.log_op(k, DELETE, "", 0); err != nil {
		return err
	}
	delete(s.data, k)

	return nil
}

func (s *store) Expire(k key, ttl time.Duration) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	val, exists := s.data[k]
	if !exists {
		return errors.New("the key does not exist")
	}

	if err := s.wal.log_op(k, EXPIRE, "", ttl); err != nil {
		return err
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
