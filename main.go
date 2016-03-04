package main

import "fmt"
import "os"
import "io/ioutil"
import "gopkg.in/yaml.v2"

type collectionDescription struct {
	Collection string
	Dependency string
	ForeignKey string `yaml:"foreign_key"`
	Filters    map[string]string
	Data       []map[string]string
}

func extractData(todo *collectionDescription) {
	fmt.Printf("Extracting data from collection %s\n", todo.Collection)
}

func main() {
	bytes, _ := ioutil.ReadFile(os.Args[1])
	collections := []*collectionDescription{}
	doneCollectionCount := 0
	yaml.Unmarshal([]byte(bytes), &collections)
	doneCollections := make(map[string]bool)
	todo := []*collectionDescription{}
	for {
		for _, item := range collections {
			if !doneCollections[item.Collection] && (len(item.Dependency) == 0 || doneCollections[item.Dependency]) {
				todo = append(todo, item)
			}
		}

		for {
			var item *collectionDescription
			//fmt.Printf("Size of todo %d\n", len(todo))
			item, todo = todo[0], todo[1:]
			//fmt.Printf("Size of todo %d\n", len(todo))
			extractData(item)
			doneCollections[item.Collection] = true
			doneCollectionCount += 1
			//fmt.Printf("Size of todo %d\n", len(todo))
			//fmt.Printf("Done collections %d\n", doneCollectionCount)
			if len(todo) == 0 {
				break
			}
		}
		//fmt.Printf("--- Done collections %d\n", doneCollectionCount)
		if doneCollectionCount == len(collections) {
			break
		}
	}

}
