package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type TigType int

const (
	BloobType TigType = iota
	TreeType
)

type EntryTyper interface {
	EntryType() string
	String() string
}

type Entry struct {
	sync.Mutex
	Mode   uint32
	Type   TigType
	Sha1   string
	Name   string
	Parent string
	Size   uint64
}

func (e *Entry) EntryType() string {
	return e.Type.String()
}

type Tree struct {
	Entry
	Entries []EntryTyper
}

func (tt TigType) String() string {
	if tt == BloobType {
		return "bloob"
	}
	return "tree"
}

func (e *Entry) String() string {
	return fmt.Sprintf("%d %s %s   %s", e.Mode, e.Type.String(), e.Sha1, e.Name)
}

type Indexer struct {
	Trees     map[string]*Tree
	wg        sync.WaitGroup
	EntryChan chan *Entry
}

func NewIndexer() *Indexer {
	i := Indexer{}
	i.EntryChan = make(chan *Entry, 10)
	i.Trees = make(map[string]*Tree)
	return &i
}

func (i *Indexer) walk(root string) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		i.wg.Add(1)
		defer i.wg.Done()

		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		go func() {
			i.index(path, info)
		}()

		return nil
	})

	if err != nil {
		fmt.Printf("%v\n", err)
	}

	return err
}

func (i *Indexer) index(fp string, info os.FileInfo) {
	i.wg.Add(1)
	defer i.wg.Done()

	f, err := os.Open(fp)
	if err != nil {
		log.Fatal(err.Error())
	}

	shaHasher := sha1.New()
	if _, err := io.Copy(shaHasher, f); err != nil {
		log.Fatal(err.Error())
	}
	f.Close()

	e := &Entry{}
	e.Parent = filepath.Dir(fp)
	e.Name = info.Name()
	e.Mode = uint32(info.Mode())
	e.Type = BloobType
	e.Size = uint64(info.Size())
	e.Sha1 = hex.EncodeToString(shaHasher.Sum(nil))

	i.EntryChan <- e
}

func (i *Indexer) hoist() {
	for _, v := range i.Trees {
		p := i.Trees[v.Parent]
		if p != nil {
			p.Entries = append(p.Entries, v)
		}
	}
}

func main() {
	arg := flag.String("p", ".", "path from where to start indexing")
	flag.Parse()

	dir, err := filepath.Abs(path.Clean(*arg))
	if err != nil {
		panic(err)
	}
	indexer := NewIndexer()
	now := time.Now()

	indexer.walk(dir)

	go func() {
		indexer.wg.Wait()
		indexer.EntryChan <- nil
	}()

	count := 0
	bytes := uint64(0)

	root := &Tree{}
	root.Name = filepath.Base(dir)
	root.Parent = filepath.Dir(dir)
	root.Type = TreeType
	indexer.Trees[root.Parent] = root

	hasMore := true
	for hasMore {
		select {
		case entry := <-indexer.EntryChan:
			if entry == nil {
				hasMore = false
				break
			}

			t, ok := indexer.Trees[entry.Parent]
			if !ok {
				t = &Tree{}
				t.Name = filepath.Base(entry.Parent)
				t.Parent = filepath.Dir(entry.Parent)
				t.Type = TreeType
				t.Entries = make([]EntryTyper, 5)
				indexer.Trees[entry.Parent] = t
			}

			count++
			bytes += entry.Size
			t.Entries = append(t.Entries, entry)
			fmt.Printf("[%d] %s %s\n", count, filepath.Join(entry.Parent, entry.Name), entry.Sha1)
			break
		}
	}

	fmt.Printf("[END] %s files (%s) in %s\n", humanize.Comma(int64(count)), humanize.Bytes(bytes), time.Since(now))
	fmt.Println()
}
