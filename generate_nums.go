package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"./shared"
)

const option = 1

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

	start := time.Now().Unix()
	genNums(n)
	end := time.Now().Unix()

	fmt.Print("  100%   \r")
	fmt.Printf("\ndone! took %d seconds\n", end-start)
}

func genNums(total int) {
	w := shared.NewWriter("nums.txt")

	count := 0
	for count < total {
		w.WriteIntLine(rand.Int())

		count++
		if count%1000000 == 0 {
			percent := (float64(count) / float64(total)) * 100
			fmt.Print("  "+strconv.FormatFloat(percent, 'f', 2, 32), "%\r")
		}
	}

	w.Close()
}
