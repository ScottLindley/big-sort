package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"

	"./shared"
)

func main() {
	args := os.Args
	n := 100000000
	if len(args) > 1 {
		var err error
		n, err = strconv.Atoi(args[1])
		if err != nil {
			log.Fatal(`number argument malformed`)
		}
	}

	fmt.Printf("generating %d random numbers\n", n)

	<-shared.WriteLines("nums.txt",
		shared.IntsToLines(
			generateNums(n)))

	fmt.Println("\ndone!")
}

func generateNums(total int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		count := 0
		for count < total {
			out <- rand.Int()
			count++
			if count%1000000 == 0 {
				percent := (float64(count) / float64(total)) * 100
				fmt.Print("  "+strconv.FormatFloat(percent, 'f', 2, 32), "%\r")
			}
		}
	}()

	return out
}
