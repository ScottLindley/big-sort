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

	shared.DeleteDirectory("temp")
	shared.CreateDirectory("temp")

	mergedFile := <-mergeFiles(sortFiles(split("nums.txt", 1000000)))

	err := os.Rename(mergedFile, "sorted.txt")
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

				w := shared.NewWriter(pathC)
				rA := shared.NewReader(pathA)
				rB := shared.NewReader(pathB)

				// indicates that a/b is closed -- no more values
				doneA := false
				doneB := false

				nA, ok := rA.ReadLineInt()
				if !ok {
					doneA = true
				}
				nB, ok := rB.ReadLineInt()
				if !ok {
					doneB = true
				}

				// so long as we have more values
				for !doneA || !doneB {
					if !doneA && (doneB || nA < nB) {
						n, ok := rA.ReadLineInt()
						if !ok {
							doneA = true
							w.WriteIntLine(nB)
						} else {
							w.WriteIntLine(nA)
							nA = n
						}
					} else {
						n, ok := rB.ReadLineInt()
						if !ok {
							doneB = true
							w.WriteIntLine(nA)
						} else {
							w.WriteIntLine(nB)
							nB = n
						}
					}
				}

				rA.Close()
				rB.Close()
				w.Close()

				// clean up the two files
				shared.DeleteFile(pathA)
				shared.DeleteFile(pathB)

				out <- pathC
			}
		}()

		return out
	}

	merged := shared.FanIn(shared.FanOut(workerFn, 20))

	mergedFiles := make([]string, 0)
	for {
		f, ok := <-merged
		if !ok {
			break
		}
		mergedFiles = append(mergedFiles, f)
	}

	if len(mergedFiles) > 1 {
		return mergeFiles(shared.StringStream(mergedFiles))
	}
	return shared.StringStream(mergedFiles)
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

				r := shared.NewReader(filePath)
				nums := r.ReadAllInts()
				sort.Ints(nums)

				w := shared.NewWriter(filePath)
				for _, n := range nums {
					w.WriteIntLine(n)
				}

				r.Close()
				w.Close()

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
		lines := 0

		r := shared.NewReader(filePath)
		path := genFilePath(fileCount)
		w := shared.NewWriter(path)

		for {
			line, ok := r.ReadLineString()
			if !ok {
				r.Close()
				w.Close()
				filePaths <- path
				return
			}

			w.Write([]byte(line))

			lines++
			if lines > lineCount {
				filePaths <- path
				fileCount++
				w.Close()
				path = genFilePath(fileCount)
				w = shared.NewWriter(path)
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
