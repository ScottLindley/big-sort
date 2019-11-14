# Big Sort!

Sorting numbers is pretty straight forward, but what if the list of numbers doesn't fit into memory?

`sort.go` will sort a file of numbers of any size!

### Generate some numbers

To generate a large file of numbers, run
`go run generate_nums.go`

The script will default to 100,000,000 numbers which works out to be about a 2GB file. If you'd like to change the file size, you can pass an argument to control the number of random nums. Ex: `go run generate_nums.go 9999`

Your file of number will be written out `nums.txt`

### Sort

To sort you file, simply run `go run sort.go`

Once the script is finished, you can expect to find a new file named `sorted.txt`

### Verify

Don't believe me? To verify that `sorted.txt` is indeed sorted you can run `go run check_sorted_list.go`

### How it works

The program functions as a pipeline where each step operates concurrently. I'll lay out the steps below.

**Split:** by reading the original file in chunks, split the original file into smaller files that **will** fit into memory

**Sort:** load the split files into memory, sort the numbers, and write the file back out to the same address.

**Merge:** take two sorted files, A and B, and read them line by line starting at the top. If the number at line A is less than the number at line B, append line A to a new merged file and advance file A to the next line, else write and advance B.

After the files from the sort step are all merged, are left with `n sorted files / 2`. The merged files are then sent back through the merge step until there is only one file remaining -- that's how we know we're done!