package shared

import (
	"bufio"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Writer struct {
	FilePath     string
	File         *os.File
	Buffer       *bufio.Writer
	BytesWritten int64
}

func NewWriter(filePath string) *Writer {
	w := Writer{FilePath: filePath}
	w.Open()
	return &w
}

func (w *Writer) Open() {
	f, err := os.Create(w.FilePath)
	if err != nil {
		panic(err)
	}
	w.File = f
	w.Buffer = bufio.NewWriterSize(f, 1024*1024)
}

func (w *Writer) Close() {
	if w.File != nil {
		w.Buffer.Flush()
		w.File.Close()
		w.File = nil
		w.Buffer = nil
	}
}

func (w *Writer) Write(b []byte) {
	if w.Buffer != nil {
		n, err := w.Buffer.Write(b)
		if err != nil {
			panic(err)
		}
		w.BytesWritten += int64(n)
	}
}

func (w *Writer) WriteIntLine(n int) {
	if w.Buffer != nil {
		line := strconv.Itoa(n) + "\n"
		n, err := w.Buffer.WriteString(line)
		if err != nil {
			panic(err)
		}
		w.BytesWritten += int64(n)
	}
}

type Reader struct {
	FilePath  string
	File      *os.File
	Scanner   *bufio.Scanner
	BytesRead int64
}

func NewReader(filePath string) *Reader {
	r := Reader{FilePath: filePath}
	r.Open()
	return &r
}

func (r *Reader) Open() {
	f, err := os.Open(r.FilePath)
	if err != nil {
		panic(err)
	}
	r.File = f
}

func (r *Reader) Close() {
	if r.File != nil {
		r.File.Close()
		r.File = nil
		r.Scanner = nil
	}
}

func (r *Reader) ReadLine() ([]byte, bool) {
	if r.Scanner == nil {
		r.Scanner = bufio.NewScanner(r.File)
	}

	if r.Scanner.Scan() {
		b := r.Scanner.Bytes()
		r.BytesRead += int64(len(b))
		return b, true
	}
	return []byte{}, false
}

func (r *Reader) ReadLineString() (string, bool) {
	b, ok := r.ReadLine()
	if !ok {
		return "", false
	}
	return string(b) + "\n", true
}

func (r *Reader) ReadLineInt() (int, bool) {
	b, ok := r.ReadLine()
	if !ok {
		return -1, false
	}
	n, err := strconv.Atoi(string(b))
	if err != nil {
		panic(err)
	}
	return n, true
}

func (r *Reader) ReadAll() []byte {
	b, err := ioutil.ReadAll(r.File)
	if err != nil {
		panic(err)
	}
	return b
}

func (r *Reader) ReadAllInts() []int {
	s := string(r.ReadAll())
	lines := strings.Split(s, "\n")
	nums := make([]int, 0)
	for _, line := range lines {
		if line != "" {
			n, err := strconv.Atoi(line)
			if err != nil {
				panic(err)
			}
			nums = append(nums, n)
		}
	}
	return nums
}

func DeleteFile(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return
	}
	var err = os.Remove(path)
	if err != nil {
		panic(err)
	}
}

func CreateDirectory(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, 0711)
		if err != nil {
			panic(err)
		}
	}
}

func DeleteDirectory(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return
	}
	var err = os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

func LinesToInts(in <-chan []byte) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for {
			b, ok := <-in
			if !ok {
				return
			}

			n, err := strconv.Atoi(string(b))
			if err != nil {
				panic(err)
			}

			out <- n
		}
	}()

	return out
}

func StringStream(strs []string) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)
		for _, s := range strs {
			out <- s
		}
	}()

	return out
}

func IntStream(nums []int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for _, n := range nums {
			out <- n
		}
	}()

	return out
}

func IntsToBytes(in <-chan int) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)
		for {
			n, ok := <-in
			if !ok {
				return
			}
			out <- []byte(strconv.Itoa(n))
		}
	}()

	return out
}

func IntsToLines(in <-chan int) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)
		for {
			n, ok := <-in
			if !ok {
				return
			}
			out <- []byte(strconv.Itoa(n) + "\n")
		}
	}()

	return out
}

func Batch(size int, in <-chan string) <-chan []string {
	out := make(chan []string)
	go func() {
		defer close(out)
		for {
			batch := make([]string, 0)

			for i := 0; i < size; i++ {
				select {
				case s, ok := <-in:

					if !ok {
						out <- batch
						return
					}
					batch = append(batch, s)
				}
			}

			out <- batch
		}
	}()
	return out
}

func FanOut(f func() <-chan string, workers int) []<-chan string {
	workerChans := make([]<-chan string, workers)
	for i := 0; i < workers; i++ {
		workerChans[i] = f()
	}
	return workerChans
}

func FanIn(channels []<-chan string) <-chan string {
	var wg sync.WaitGroup
	multiplexedStreams := make(chan string)

	multiplex := func(c <-chan string) {
		defer wg.Done()
		for {
			i, ok := <-c
			if !ok {
				return
			}
			multiplexedStreams <- i
		}
	}

	wg.Add(len(channels))
	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		defer close(multiplexedStreams)
		wg.Wait()
	}()

	return multiplexedStreams
}
