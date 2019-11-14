package main

import (
	"fmt"
	"log"
	"strconv"

	"./shared"
)

func main() {
	fileName := "sorted.txt"
	fileSize := shared.GetFileSize(fileName)

	r := shared.NewReader(fileName)
	i, _ := r.ReadLineInt()

	for {
		j, ok := r.ReadLineInt()
		if !ok {
			break
		}
		printProgress(r.BytesRead, fileSize)
		if i > j {
			log.Fatal(fmt.Sprintf("Not properly sorted! %d came before %d", i, j))
		}

		i = j
	}

	fmt.Print("  100%  \r")
	fmt.Println("\nDone - list is properly sorted!")
}

func printProgress(read int64, total int64) {
	if read%1000000 == 0 {
		percent := (float64(read) / float64(total)) * 100
		fmt.Print("  "+strconv.FormatFloat(percent, 'f', 2, 32), "%\r")
	}
}
