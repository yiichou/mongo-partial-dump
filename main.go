package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"reflect"
)

type collectionDescription struct {
	Collection string
	Dependency string
	ForeignKey string `yaml:"foreign_key"`
	Filters    bson.M
}

func extractData(description *collectionDescription, dependentCollection *collectionDescription, sourceDb *mgo.Database, destDb *mgo.Database) {

	sourceCol := sourceDb.C(description.Collection)
	if dependentCollection != nil {
		fmt.Printf("Extracting data from collection %s using key %s related to %s\n", description.Collection, description.ForeignKey, dependentCollection.Collection)
		depCol := destDb.C(dependentCollection.Collection)
		iterDepCol := depCol.Find(bson.M{}).Iter()
		batchSize := 50
		obj := bson.M{}
		objects := []bson.ObjectId{}
		for iterDepCol.Next(&obj) {
			objects = append(objects, obj["_id"].(bson.ObjectId))
			if len(objects) >= batchSize {
				//TODO merge with existing filters
				iter := sourceCol.Find(bson.M{description.ForeignKey: bson.M{"$in": objects}}).Iter()
				for iter.Next(&obj) {
					destDb.C(description.Collection).Insert(obj)
				}
				objects = []bson.ObjectId{}
			}
		}

		//TODO refactor this
		if len(objects) > 0 {
			iter := sourceCol.Find(bson.M{description.ForeignKey: bson.M{"$in": objects}}).Iter()
			for iter.Next(&obj) {
				destDb.C(description.Collection).Insert(obj)
			}
			objects = []bson.ObjectId{}
		}

	} else {
		fmt.Printf("%#i \n", description.Filters)
		if reflect.TypeOf(description.Filters["_id"]) == reflect.TypeOf("") {
			newId := bson.ObjectIdHex(description.Filters["_id"].(string))
			description.Filters["_id"] = newId
		}
		count, _ := sourceCol.Find(description.Filters).Count()
		fmt.Printf("%d \n\n\n", count)
		result := bson.M{}
		iter := sourceCol.Find(description.Filters).Iter()
		for iter.Next(&result) {
			destDb.C(description.Collection).Insert(result)
		}
		fmt.Printf("Extracting data from collection %s\n", description.Collection)
	}

}

func main() {

	//TODO: use environment variables
	sourceCon, _ := mgo.Dial("127.0.0.1:27017")
	destCon, _ := mgo.Dial("127.0.0.1:27017")
	defer sourceCon.Close()
	defer destCon.Close()

	db1 := sourceCon.DB(os.Getenv("SOURCE_DB"))
	db2 := destCon.DB(os.Getenv("DEST_DB"))

	bytes, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	var collections []*collectionDescription
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
			var dependencyCollection *collectionDescription
			if len(item.Dependency) > 0 {
				for _, other := range collections {
					if other.Collection == item.Dependency {
						dependencyCollection = other
						break
					}
				}
			}

			extractData(item, dependencyCollection, db1, db2)
			doneCollections[item.Collection] = true
			doneCollectionCount += 1
			if len(todo) == 0 {
				break
			}
		}

		if doneCollectionCount == len(collections) {
			break
		}
	}

}
