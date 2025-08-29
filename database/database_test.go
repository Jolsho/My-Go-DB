package database

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
	"mydb/core/pages"
	"mydb/fileT"
)

func Test_File_Table(t *testing.T) {
	db := new(Database)
	err := db.Start("test.db")
	if err != nil {
		t.Fatalf("Failed to start database: %v", err)
		return
	}

	defer db.Close()
	defer os.Remove("test.db")

	var size, ti int64
	var fileType int8
	var hash [32]byte
	var now time.Time
	var idd string
	var inTotal, delTotal, getTotal time.Duration

	uidbytes, _ := hex.DecodeString("ABCABC123123ABC1ABCABC123123ABC1")
	uid := hex.EncodeToString(uidbytes)

	// I believe for FID index is something like 98...	
	// its expected that we get a clean call followed by a split call
	// split call will also call new root node and shit
	const ITERATIONS = 100000
	const LOOP = 2
	const LAP = 10000
	const BREAKER = 10
	const STARTER = 0
	i := 0
	for i = range ITERATIONS {

		if i % LAP == 0 && i != 0 {
			fmt.Printf("\n@ %d ITERATIONS", i)
			PrintStats(inTotal,delTotal,getTotal,LAP,LOOP,db.Total)
			inTotal = 0
			delTotal = 0
			getTotal = 0
		}
		if i % LOOP == 0 && i != 0 { 
			// DELETE THA MU FUGA
			idd = fmt.Sprintf("randomvalue%d", i-2)
			hash = sha256.Sum256([]byte(idd))
			now = time.Now()
			err = db.DeleteFile(uid, hash)
			if err != nil {
				PrintStats(inTotal,delTotal,getTotal,int64(LAP%i),LOOP,db.Total)
				t.Fatalf("Failed to DELETE file: %v #%d", err, i-2)
				return
			}
			delTotal += time.Since(now)

			// GET ANOTHA MU FUGA
			idd = fmt.Sprintf("randomvalue%d", i-1)
			hash = sha256.Sum256([]byte(idd))
			now = time.Now()
			h,_,err := db.GetFile(uid, hash)
			if err != nil || h.Id != hex.EncodeToString(hash[:16])  {
				if err != nil {
					PrintStats(inTotal,delTotal,getTotal,int64(LAP%i),LOOP,db.Total)
					t.Fatalf("Failed to GET file: %v #%d", err, i-1)
				} else {
					t.Fatalf("Failed to GET file: %v #%d", "invalid id gotten", i-1)
				}
				return
			}


			getTotal += time.Since(now)
		}

		size = int64(1024+i)
		fileType = fileT.Jpeg
		idd = fmt.Sprintf("randomvalue%d", i)
		hash = sha256.Sum256([]byte(idd))
		ti = int64(1633036800-i) // Example timestamp

		now = time.Now()

		err = db.InsertFile(uid, size, fileType, hash, ti)
		if err != nil {
			PrintStats(inTotal,delTotal,getTotal,int64(LAP%i+1),LOOP,db.Total)
			t.Fatalf("Failed to insert file: %v #%d", err, i)
			return
		}
		inTotal += time.Since(now)

		// if i > STARTER {
		// 	fmt.Printf("%d ", i)
		// }
	}

	fmt.Println("\n@", ITERATIONS, "ITERATIONS")
	PrintStats(inTotal,delTotal,getTotal,LAP,LOOP,db.Total)

}

func PrintStats(inTotal, delTotal, getTotal time.Duration, ITERATIONS, LOOP int64, pageCount uint64) {
	fmt.Printf("\n")
	fmt.Printf(" AVG-INSERT: %03d micros\n", inTotal.Microseconds()/(ITERATIONS))
	fmt.Printf(" AVG-DELETE: %03d micros\n", delTotal.Microseconds()/((ITERATIONS/LOOP)-1))
	fmt.Printf(" AVG-GET:    %03d micros\n", getTotal.Microseconds()/(ITERATIONS/LOOP)-1)
	fmt.Printf(" TOTAL PAGE: %03d\n", pageCount) 
	fmt.Printf(" TOTAL SIZE: %dkb\n", (pageCount*uint64(pages.PAGE_SIZE))/1024) 
	printMemStats()
	fmt.Printf("\n")
}
func printMemStats() {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
	fmt.Printf("\n")
	fmt.Printf(" ALLOC:	%v MiB\n", m.Alloc/1024/1024)
	fmt.Printf(" TOTAL:	%v MiB\n", m.TotalAlloc/1024/1024)
	fmt.Printf(" SYS:	%v MiB\n", m.Sys/1024/1024)
	fmt.Printf(" NUMGC:	%v\n", m.NumGC)
	fmt.Printf(" OBJEX:	%v\n", m.HeapObjects)
}
