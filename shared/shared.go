package shared

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

func WriteLines(path string, in <-chan []byte) <-chan bool {
	out := make(chan bool)

	go func() {
		defer close(out)
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		r := bufio.NewWriterSize(f, 1024*1024)

		defer r.Flush()

		for {
			bytes, ok := <-in
			if !ok {
				return
			}
			bytes = []byte(string(bytes))
			_, err := r.Write(bytes)
			if err != nil {
				panic(err)
			}
		}
	}()

	return out
}

func ReadLines(path string) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		s := bufio.NewScanner(f)

		for s.Scan() {
			lines := strings.Split(s.Text(), "\n")
			for _, line := range lines {
				if line != "" {
					out <- []byte(line)
				}
			}
		}

		err = s.Err()
		if err != nil {
			panic(err)
		}
	}()

	return out
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

func GetFileSize(path string) int64 {
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		fmt.Printf("Get file size %s does not exist\n", path)
		return 0
	}
	if err != nil {
		panic(err)
	}
	return fi.Size()
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
