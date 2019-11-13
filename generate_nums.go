package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
)

func main() {
	args := os.Args
	n := 100000000
	if len(args) > 1 {
		var err error
		n, err = strconv.Atoi(args[1])
		if err != nil {
			panic(err)
			log.Fatal(`number argument malformed`)
		}
	}

	fmt.Printf("generating %d random numbers\n", n)

	<-writeBatch(batchNums(80, generateNums(n)))

	fmt.Println("\ndone!")
}

func generateNums(total int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		count := 0
		for count < total {
			out <- rand.Int()
			count = count + 1
			if count%1000000 == 0 {
				percent := (float64(count) / float64(total)) * 100
				fmt.Print("  "+strconv.FormatFloat(percent, 'f', 2, 32), "%\r")
			}
		}
	}()

	return out
}

func batchNums(batchSize int, in <-chan int) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)
		batch := ""
		count := 0

		sendBatch := func() {
			out <- batch
			batch = ""
			count = 0
		}

		for {
			n, ok := <-in
			if !ok {
				sendBatch()
				return
			}
			batch = batch + strconv.Itoa(n) + "\n"
			count = count + 1
			if count >= batchSize {
				sendBatch()
			}
		}
	}()

	return out
}

func writeBatch(in <-chan string) <-chan bool {
	out := make(chan bool)

	go func() {
		defer close(out)
		f, err := os.Create("nums.txt")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		for {
			chunk, ok := <-in
			if !ok {
				out <- true
				return
			}

			_, err := f.WriteString(chunk)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	return out
}
