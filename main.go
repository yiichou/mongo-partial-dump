package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

type collectionDescription struct {
	Collection   string
	Dependency   string
	ForeignKey   string `yaml:"foreign_key"`
	ReferenceKey string `yaml:"reference_key"`
	Filters      bson.M
}

var syncedDocumentIds = make(map[string][]bson.ObjectId)

func correctFilters(filters bson.M) bson.M {
	for key, value := range filters {
		if strings.HasSuffix(key, "_id") && reflect.TypeOf(filters[key]) == reflect.TypeOf("") {
			filters[key] = bson.ObjectIdHex(value.(string))
		} else if reflect.ValueOf(value).Kind() == reflect.Slice {
			filters[key] = bson.M{"$in": value}
		}
	}
	return filters
}

func batchSlice(slice []bson.ObjectId, batchSize int) [][]bson.ObjectId {
	var batches [][]bson.ObjectId
	for batchSize < len(slice) {
		slice, batches = slice[batchSize:], append(batches, slice[0:batchSize:batchSize])
	}
	batches = append(batches, slice)
	return batches
}

func extractAndInsertDocuments(objectIds []bson.ObjectId, collectionDescription *collectionDescription, sourceCollection *mgo.Collection, destDb *mgo.Database) {
	criteria := bson.M{}
	if objectIds != nil && len(collectionDescription.ForeignKey) > 0 {
		criteria = bson.M{collectionDescription.ForeignKey: bson.M{"$in": objectIds}}
	}
	if objectIds != nil && len(collectionDescription.ReferenceKey) > 0 {
		criteria = bson.M{"_id": bson.M{"$in": objectIds}}
	}
	for key, value := range collectionDescription.Filters {
		criteria[key] = value
	}

	criteria = correctFilters(criteria)
	fmt.Printf("Criteria %s\n", criteria)

	destCollection := destDb.C(collectionDescription.Collection)
	destCollection.RemoveAll(criteria)

	obj := bson.M{}
	iter := sourceCollection.Find(criteria).Iter()
	for iter.Next(&obj) {
		destCollection.Insert(obj)
		syncedDocumentIds[collectionDescription.Collection] = append(syncedDocumentIds[collectionDescription.Collection], obj["_id"].(bson.ObjectId))
		fmt.Printf(".")
	}
	fmt.Printf("\n")
}

func extractData(description *collectionDescription, dependentCollection *collectionDescription, sourceDb *mgo.Database, destDb *mgo.Database) {
	sourceCol := sourceDb.C(description.Collection)
	if len(description.ForeignKey) > 0 {
		if dependentCollection != nil && len(syncedDocumentIds[dependentCollection.Collection]) > 0 {
			fmt.Printf("Extracting data from collection %s using key %s related to %s\n", description.Collection, description.ForeignKey, dependentCollection.Collection)
			for _, objectIds := range batchSlice(syncedDocumentIds[dependentCollection.Collection], 500) {
				extractAndInsertDocuments(objectIds, description, sourceCol, destDb)
			}
		}
	} else if len(description.ReferenceKey) > 0 {
		if dependentCollection != nil && len(syncedDocumentIds[dependentCollection.Collection]) > 0 {
			fmt.Printf("Extracting data from collection %s using key %s referenced by %s\n", description.Collection, description.ReferenceKey, dependentCollection.Collection)
			depCol := destDb.C(dependentCollection.Collection)
			for _, referencedIds := range batchSlice(syncedDocumentIds[dependentCollection.Collection], 500) {
				criteria := bson.M{"_id": bson.M{"$in": referencedIds}}
				objectIds := []bson.ObjectId{}
				err := depCol.Find(criteria).Distinct(description.ReferenceKey, &objectIds)
				if err == nil && len(objectIds) > 0 {
					extractAndInsertDocuments(objectIds, description, sourceCol, destDb)
				}
			}
		}
	} else {
		fmt.Printf("Extracting data from collection %s\n", description.Collection)
		extractAndInsertDocuments(nil, description, sourceCol, destDb)
	}
}

func createDBConnection(uri *url.URL) (session *mgo.Session, db *mgo.Database) {
	session, _ = mgo.Dial(uri.Host)
	db = session.DB(uri.Path[1:])
	if uri.User != nil {
		password, _ := uri.User.Password()
		username := uri.User.Username()
		if len(username) > 0 && len(password) > 0 {
			db.Login(username, password)
		}
	}

	return
}

func main() {

	sourceURI, err1 := url.Parse(os.Getenv("SOURCE_URI"))
	destURI, err2 := url.Parse(os.Getenv("DESTINATION_URI"))

	if err1 != nil || err2 != nil || sourceURI.Host == "" || destURI.Host == "" {
		panic("You must define both SOURCE_URI and DESTINATION_URI env variables according to MongoDB connection string URI format. See https://docs.mongodb.org/master/reference/connection-string/")
	}

	sourceCon, db1 := createDBConnection(sourceURI)
	destCon, db2 := createDBConnection(destURI)

	defer sourceCon.Close()
	defer destCon.Close()

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
			if len(todo) == 0 {
				doneCollectionCount += 1
				break
			}

			var item *collectionDescription
			// fmt.Printf("Size of todo %d\n", len(todo))
			item, todo = todo[0], todo[1:]
			// fmt.Printf("Size of todo %d\n", len(todo))
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

		fmt.Printf("doneCollectionCount %d\n", doneCollectionCount)
		fmt.Printf("Size of collections %d\n", len(collections))
		if doneCollectionCount == len(collections) {
			break
		}
	}

}
