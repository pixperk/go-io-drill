package main

import (
	"os"
)

type file struct {
	name    string
	content string
	size    int
}

func newFile(name string) (*file, error) {

	content, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	fstat, err := os.Stat(name)
	if err != nil {
		return nil, err
	}

	return &file{
		name:    name,
		content: string(content),
		size:    int(fstat.Size()),
	}, nil
}

// opens a file (without buffer), reads content, writes new content, and fsyncs it to disk
// returns the old read content, whether fsync was successful, and any error encountered
func (f *file) open_read_write_and_fsync_if_needed(content_to_write string, fsync_wanted bool) (string, bool, error) {
	fd, err := os.OpenFile(f.name, os.O_RDWR, 0644)
	if err != nil {
		return "", false, err
	}

	defer fd.Close()

	// read existing content
	buf := make([]byte, f.size)
	n := 0
	for n < f.size {
		nn, err := fd.Read(buf[n:])
		if err != nil {
			return "", false, err
		}
		n += nn
	}

	readContent := string(buf)

	//write/truncate new content
	_, err = fd.Seek(0, 0)
	if err != nil {
		return "", false, err
	}

	if len(content_to_write) < f.size {
		err = fd.Truncate(int64(len(content_to_write)))
		if err != nil {
			return readContent, false, err
		}
	} else {
		_, err = fd.Write([]byte(content_to_write))
		if err != nil {
			return readContent, false, err
		}
	}

	if fsync_wanted {
		err = fd.Sync()
		if err != nil {
			return readContent, false, err
		}

		return readContent, true, nil
	}

	return readContent, false, nil

}

func main() {

	f, err := newFile("notes.txt")
	if err != nil {
		panic(err)
	}

	content_to_write := "hello, overwriting some bse bs"
	fsync_wanted := true

	new_content, fsynced, err := f.open_read_write_and_fsync_if_needed(content_to_write, fsync_wanted)
	if err != nil {
		panic(err)
	}

	if fsynced {
		println("File content after write and fsync:", new_content)
	} else {
		println("File content after write (no fsync):", new_content)
	}
}
