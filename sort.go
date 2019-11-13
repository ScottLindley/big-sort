package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	start := time.Now().Unix()
	fileName := <-mergeFiles(sortFiles(split("nums.txt", 1000000)))
	end := time.Now().Unix()
	fmt.Printf("finished in %d seconds: %s", end-start, fileName)
}

func mergeFiles(in <-chan string) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)
		i := 0
		pathA := ""
		pathB := ""

		for {
			path, ok := <-in
			if !ok {
				out <- pathA
				return
			}

			if pathA == "" {
				pathA = path
				continue
			}

			pathB = path
			pathC := "sorted_" + strconv.Itoa(i) + ".txt"
			i++

			w := make(chan int)
			writeLine(pathC, intsToLines(w))

			readA := linesToInts(readLine(pathA))
			readB := linesToInts(readLine(pathB))

			// indicates that a/b is closed -- no more values
			doneA := false
			doneB := false

			nA, ok := <-readA
			if !ok {
				doneA = true
			}
			nB, ok := <-readB
			if !ok {
				doneB = true
			}

			// so long as we have more values
			for !doneA || !doneB {
				if !doneA && (doneB || nA < nB) {
					n, ok := <-readA
					if !ok {
						doneA = true
						w <- nB
					} else {
						w <- nA
						nA = n
					}
				} else {
					n, ok := <-readB
					if !ok {
						doneB = true
						w <- nA
					} else {
						w <- nB
						nB = n
					}
				}
			}

			// clean up the two files
			go deleteFile(pathA)
			go deleteFile(pathB)

			pathA, pathB = pathC, ""
		}
	}()

	return out
}

func sortFiles(in <-chan string) <-chan string {
	workerFn := func() <-chan string {
		out := make(chan string)
		go func() {
			defer close(out)
			for {
				filePath, ok := <-in
				if !ok {
					return
				}

				nums := make([]int, 0)
				c := linesToInts(readLine(filePath))
				for {
					n, ok := <-c
					if !ok {
						break
					}
					nums = append(nums, n)
				}

				sort.Ints(nums)

				<-writeLine(filePath, intsToBytes(nums))

				out <- filePath
			}
		}()
		return out
	}

	// run sorters in parallel
	return fanIn(fanOut(workerFn, 8))
}

func split(filePath string, lineCount int) <-chan string {
	filePaths := make(chan string)

	go func() {
		defer close(filePaths)

		pieces := strings.Split(filePath, ".")
		fileName := pieces[0]
		fileExtension := pieces[1]

		genFilePath := func(n int) string {
			return fileName + "_" + strconv.Itoa(n) + "." + fileExtension
		}

		fileCount := 0
		in := readLine(filePath)
		out := make(chan []byte)
		path := genFilePath(fileCount)
		writeLine(path, out)
		lines := 0

		for {
			line, ok := <-in
			if !ok {
				close(out)
				filePaths <- path
				return
			}
			out <- line

			lines++
			if lines > lineCount {
				close(out)
				out = make(chan []byte)
				filePaths <- path
				fileCount++
				path = genFilePath(fileCount)
				writeLine(path, out)
				lines = 0
			}
		}
	}()

	return filePaths
}

func writeLine(path string, in <-chan []byte) <-chan bool {
	out := make(chan bool)

	go func() {
		defer close(out)
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		batch := make([]byte, 0)

		for {
			bytes, ok := <-in
			if !ok {
				return
			}
			bytes = []byte(string(bytes) + "\n")
			for _, b := range bytes {
				batch = append(batch, b)
			}
			if len(batch) > 1024*1024 {
				_, err := f.Write(batch)
				if err != nil {
					panic(err)
				}
				batch = make([]byte, 0)
			}
		}
	}()

	return out
}

func readLine(path string) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)
		f, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		b := make([]byte, 1024*1024)

		for {
			_, err = f.Read(b)
			if err != nil {
				if err == io.EOF {
					return
				}
				panic(err)
			}

			lines := strings.Split(string(b), "\n")
			for _, line := range lines {
				if line != "" {
					out <- []byte(line)
				}
			}
		}
	}()

	return out
}

func deleteFile(path string) {
	var err = os.Remove(path)
	if err != nil {
		panic(err)
	}
}

func linesToInts(in <-chan []byte) <-chan int {
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

func intsToLines(in <-chan int) <-chan []byte {
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

func intsToBytes(nums []int) <-chan []byte {
	out := make(chan []byte)

	go func() {
		defer close(out)
		for _, n := range nums {
			out <- []byte(strconv.Itoa(n))
		}
	}()

	return out
}

func fanOut(f func() <-chan string, workers int) []<-chan string {
	workerChans := make([]<-chan string, workers)
	for i := 0; i < workers; i++ {
		workerChans[i] = f()
	}
	return workerChans
}

func fanIn(channels []<-chan string) <-chan string {
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
