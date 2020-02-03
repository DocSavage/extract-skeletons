package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/janelia-flyem/dvid-go-kit/keyvalue"
)

const dirname = "skeletons"

func main() {
	startTime := time.Now()
	records := loadCSV("traced-neurons.csv")
	keys := make([]string, len(records))
	for i, record := range records[1:] {
		keys[i] = record[0] + "_swc"
	}

	keyvalue.SetDebug(true)
	numRead, _, _, err := keyvalue.ProcessKeyValues("http://emdata4:8900/api/node/52a13/segmentation_skeletons/", keys, writeSkeleton)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Extracted %d skeletons: %s\n", numRead, time.Since(startTime))
}

func loadCSV(csvName string) (records [][]string) {
	csvF, err := os.Open(csvName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := csv.NewReader(csvF)
	records, err = r.ReadAll()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Got %d lines with %d fields\n", len(records), len(records[0]))
	return
}

func writeSkeleton(kv *keyvalue.KeyValue, storeChan chan<- *keyvalue.KeyValue, deleteChan chan<- string) {
	if len(kv.Value) != 0 {
		bodyID := strings.TrimSuffix(kv.Key, "_swc")
		fname := fmt.Sprintf("%s/%s.swc", dirname, bodyID)
		outF, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			fmt.Printf("unable to write skeleton file for key %s: %v\n", kv.Key, err)
			os.Exit(1)
		}
		defer outF.Close()

		if _, err := outF.Write(kv.Value); err != nil {
			fmt.Printf("unable to write skeleton file for key %s: %v\n", kv.Key, err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Key %s seems to have no entry... skipping\n", kv.Key)
	}
}
