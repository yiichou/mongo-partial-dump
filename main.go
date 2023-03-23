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
	Collection string
	Dependency string
	ForeignKey string `yaml:"foreign_key"`
	Filters    bson.M
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

func extractAndInsertDocuments(objectIds []bson.ObjectId, collectionDescription *collectionDescription, sourceCollection *mgo.Collection, destDb *mgo.Database) {
	criteria := bson.M{}
	if objectIds != nil && len(collectionDescription.ForeignKey) > 0 {
		criteria = bson.M{collectionDescription.ForeignKey: bson.M{"$in": objectIds}}
	}

	for key, value := range collectionDescription.Filters {
		criteria[key] = value
	}

	obj := bson.M{}
	criteria = correctFilters(criteria)
	fmt.Printf("Criteria %s\n", criteria)

	destDb.C(collectionDescription.Collection).RemoveAll(criteria)

	iter := sourceCollection.Find(criteria).Iter()
	for iter.Next(&obj) {
		destDb.C(collectionDescription.Collection).Insert(obj)
		syncedDocumentIds[collectionDescription.Collection] = append(syncedDocumentIds[collectionDescription.Collection], obj["_id"].(bson.ObjectId))
		fmt.Printf(".")
	}
	fmt.Printf("\n")
}

func extractData(description *collectionDescription, dependentCollection *collectionDescription, sourceDb *mgo.Database, destDb *mgo.Database) {

	sourceCol := sourceDb.C(description.Collection)
	// ensureEmptyCollection(destDb.C(description.Collection))
	if dependentCollection != nil {
		fmt.Printf("Extracting data from collection %s using key %s related to %s\n", description.Collection, description.ForeignKey, dependentCollection.Collection)
		batchSize := 500
		objectIds := []bson.ObjectId{}
		for _, id := range syncedDocumentIds[dependentCollection.Collection] {
			objectIds = append(objectIds, id)
			if len(objectIds) >= batchSize {
				extractAndInsertDocuments(objectIds, description, sourceCol, destDb)
				objectIds = []bson.ObjectId{}
			}
		}

		if len(objectIds) > 0 {
			extractAndInsertDocuments(objectIds, description, sourceCol, destDb)
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
