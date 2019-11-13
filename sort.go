package main

import (
	"crypto/md5"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"./shared"
)

func main() {
	start := time.Now().Unix()
	fileSize := shared.GetFileSize("nums.txt")

	shared.DeleteDirectory("temp")
	shared.CreateDirectory("temp")

	sorted := sortFiles(split("nums.txt", 1000000))
	c := make(chan string)

	merged := mergeFiles(c)

	go func() {
		n := 0
		for {
			filePath, ok := <-sorted
			if !ok {
				fmt.Println("sort closed", len(c))
				if len(c) == 1 {
					close(c)
				}
				return
			}
			n++
			c <- filePath
		}
	}()

	var mergedPath string
	for {
		var ok bool
		mergedPath, ok = <-merged
		if !ok {
			break
		}

		if shared.GetFileSize(mergedPath) < fileSize {
			c <- mergedPath
		} else {
			break
		}
	}

	close(c)
	err := os.Rename(mergedPath, "sorted.txt")
	shared.DeleteDirectory("temp")
	if err != nil {
		panic(err)
	}
	end := time.Now().Unix()
	fmt.Printf("finished in %d seconds\n", end-start)
}

func mergeFiles(in <-chan string) <-chan string {
	filePairs := shared.Batch(2, in)

	workerFn := func() <-chan string {
		out := make(chan string)

		go func() {
			defer close(out)

			for {
				paths, ok := <-filePairs
				if !ok {
					return
				}

				if len(paths) < 2 {
					if len(paths) > 0 {
						out <- paths[0]
					}
					continue
				}

				pathA := paths[0]
				pathB := paths[1]

				fmt.Printf("merging files %s & %s\n", pathA, pathB)

				pathC := generateMergedPathName(pathA, pathB)

				w := make(chan int)
				shared.WriteLines(pathC, shared.IntsToLines(w))

				readA := shared.LinesToInts(shared.ReadLines(pathA))
				readB := shared.LinesToInts(shared.ReadLines(pathB))

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
				shared.DeleteFile(pathA)
				shared.DeleteFile(pathB)

				out <- pathC
			}
		}()

		return out
	}

	return shared.FanIn(shared.FanOut(workerFn, 20))
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
				c := shared.LinesToInts(shared.ReadLines(filePath))
				for {
					n, ok := <-c
					if !ok {
						break
					}
					nums = append(nums, n)
				}

				sort.Ints(nums)

				<-shared.WriteLines(
					filePath,
					shared.IntsToLines(
						shared.IntStream(nums)))

				out <- filePath
			}
		}()
		return out
	}

	return shared.FanIn(shared.FanOut(workerFn, 8))
}

func split(filePath string, lineCount int) <-chan string {
	filePaths := make(chan string)

	go func() {
		defer close(filePaths)

		genFilePath := func(n int) string {
			return "temp/nums_" + strconv.Itoa(n) + ".txt"
		}

		fileCount := 0
		in := shared.ReadLines(filePath)
		out := make(chan []byte)
		path := genFilePath(fileCount)
		shared.WriteLines(path, out)
		lines := 0

		for {
			line, ok := <-in
			if !ok {
				close(out)
				filePaths <- path
				return
			}
			out <- []byte(string(line) + "\n")

			lines++
			if lines > lineCount {
				close(out)
				out = make(chan []byte)
				filePaths <- path
				fileCount++
				path = genFilePath(fileCount)
				shared.WriteLines(path, out)
				lines = 0
			}
		}
	}()

	return filePaths
}

func generateMergedPathName(pathA, pathB string) string {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(pathA+"-"+pathB)))
	return "temp/merged_" + hash + ".txt"
}
