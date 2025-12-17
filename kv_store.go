package main

import (
	"bufio"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type key struct {
	name string
}

type value struct {
	data       string
	expires_at time.Time
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

type Store struct {
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

func New_Store(wal_filename string) *Store {
	return &Store{
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

func (s *Store) Get(k key) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	val, exists := s.data[k]
	if !exists {
		return "", false
	}

	//check if key has expired
	if !val.expires_at.IsZero() && time.Now().After(val.expires_at) {
		return "", false
	}
	return val.data, true
}

func (s *Store) Set(k key, ttl time.Duration, v string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.wal.log_op(k, SET, v, ttl); err != nil {
		return err
	}

	s.data[k] = value{
		data: v,
		expires_at: func() time.Time {
			if ttl == 0 {
				return time.Time{}
			}
			return time.Now().Add(ttl)
		}(),
	}
	return nil
}

func (s *Store) Delete(k key) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.wal.log_op(k, DELETE, "", 0); err != nil {
		return err
	}
	delete(s.data, k)

	return nil
}

func (s *Store) Expire(k key, ttl time.Duration) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	val, exists := s.data[k]
	if !exists {
		return errors.New("the key does not exist")
	}

	if err := s.wal.log_op(k, EXPIRE, "", ttl); err != nil {
		return err
	}

	val.expires_at = time.Now().Add(ttl)
	s.data[k] = val
	return nil
}

func (s *Store) Exists(k key) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, exists := s.data[k]
	return exists
}

func (s *Store) Ttl(k key) (string, time.Duration, string, error) { //returns current time, ttl duration, expiry time, error
	s.lock.RLock()
	defer s.lock.RUnlock()
	empty_time := format_time_into_readable_string(time.Time{})

	val, exists := s.data[k]
	if !exists {
		return empty_time, 0, empty_time, errors.New("the key does not exist")
	}

	//check if key has expired
	if !val.expires_at.IsZero() && time.Now().After(val.expires_at) {
		return empty_time, 0, empty_time, errors.New("the key has expired")
	}
	expiry_time := val.expires_at
	var remaining_ttl time.Duration
	if expiry_time.IsZero() {
		remaining_ttl = 0
	} else {
		remaining_ttl = time.Until(expiry_time)
	}
	return format_time_into_readable_string(time.Now()), remaining_ttl, format_time_into_readable_string(expiry_time), nil
}

func (s *Store) Replay_wal() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	file, err := os.Open(s.wal.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		if err := s.replayEntry(parts); err != nil {
			return err
		}

		log.Printf("Replayed WAL entry: %s\n", line)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil

}

// replayEntry processes a WAL entry without acquiring locks or logging to WAL
// Caller must hold s.lock
func (s *Store) replayEntry(input_parts []string) error {
	cmd := strings.ToUpper(input_parts[0])

	switch cmd {
	case "SET":
		if len(input_parts) < 3 {
			return errors.New("SET command requires at least a key and a value")
		}
		key_name := input_parts[1]
		val_str := input_parts[2]
		var ttl time.Duration
		if len(input_parts) == 4 {
			var err error
			ttl, err = time.ParseDuration(input_parts[3])
			if err != nil {
				return errors.New("invalid TTL format")
			}
		}
		s.data[key{name: key_name}] = value{
			data: val_str,
			expires_at: func() time.Time {
				if ttl == 0 {
					return time.Time{}
				}
				return time.Now().Add(ttl)
			}(),
		}

	case "DELETE":
		if len(input_parts) != 2 {
			return errors.New("DELETE command requires a key")
		}
		key_name := input_parts[1]
		delete(s.data, key{name: key_name})

	case "EXPIRE":
		if len(input_parts) != 3 {
			return errors.New("EXPIRE command requires a key and a TTL")
		}
		key_name := input_parts[1]
		ttl, err := time.ParseDuration(input_parts[2])
		if err != nil {
			return errors.New("invalid ttl format")
		}
		k := key{name: key_name}
		if val, exists := s.data[k]; exists {
			val.expires_at = time.Now().Add(ttl)
			s.data[k] = val
		}

	default:
		return errors.New("Unknown command: " + cmd)
	}

	return nil
}

func (s *Store) Process(input_parts []string) error {
	cmd := strings.ToUpper(input_parts[0])

	switch cmd {
	case "SET":
		if len(input_parts) < 3 {
			return errors.New("SET command requires at least a key and a value")
		}
		key_name := input_parts[1]
		value := input_parts[2]
		var ttl time.Duration
		if len(input_parts) == 4 {
			var err error
			ttl, err = time.ParseDuration(input_parts[3])
			if err != nil {
				return errors.New("invalid TTL format")
			}
		}
		err := s.Set(key{name: key_name}, ttl, value)
		if err != nil {
			return err
		} else {
			log.Printf("Key %s set successfully\n", key_name)
		}

	case "GET":
		if len(input_parts) != 2 {
			return errors.New("GET command requires a key")
		}
		key_name := input_parts[1]
		value, exists := s.Get(key{name: key_name})
		if !exists {
			return errors.New("key does not exist")

		} else {
			log.Printf("Value for key %s: %s\n", key_name, value)
		}

	case "DELETE":
		if len(input_parts) != 2 {
			return errors.New("DELETE command requires a key")
		}
		key_name := input_parts[1]
		err := s.Delete(key{name: key_name})
		if err != nil {
			return err
		} else {
			log.Printf("Key %s deleted successfully\n", key_name)
		}

	case "EXPIRE":
		if len(input_parts) != 3 {
			return errors.New("EXPIRE command requires a key and a TTL")
		}
		key_name := input_parts[1]
		ttl, err := time.ParseDuration(input_parts[2])
		if err != nil {
			return errors.New("invalid ttl format")
		}
		err = s.Expire(key{name: key_name}, ttl)
		if err != nil {
			return err
		} else {
			log.Printf("Expiry for key %s set to %s successfully\n", key_name, ttl.String())
		}

	case "TTL":
		if len(input_parts) != 2 {
			return errors.New("TTL command requires a key")
		}
		key_name := input_parts[1]
		current_time, ttl_duration, expiry_time, err := s.Ttl(key{name: key_name})
		if err != nil {
			return err
		} else {
			log.Printf("Current time: %s, TTL duration: %s, Expiry time: %s for key %s\n", current_time, ttl_duration.String(), expiry_time, key_name)
		}

	case "EXISTS":
		if len(input_parts) != 2 {
			return errors.New("EXISTS command requires a key")
		}
		key_name := input_parts[1]
		exists := s.Exists(key{name: key_name})
		if exists {
			log.Printf("Key %s exists\n", key_name)
		} else {
			log.Printf("Key %s does not exist\n", key_name)
		}

	default:
		return errors.New("Unknown command: " + cmd)

	}

	return nil
}

func format_time_into_readable_string(t time.Time) string {
	if t.IsZero() {
		return "No Expiry"
	}
	return t.Format(time.RFC1123)
}
