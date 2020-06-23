package main

import (
	"context"
	"fmt"
	"github.com/rwynn/monstache/monstachemap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strings"
	"sync"
)

var warnLog = log.New(os.Stdout, "WARN ", log.Flags())

var assetFields = map[string]bool{
	"_id":             true,
	"org":             true,
	"filename":        true,
	"filetype":        true,
	"content":         true,
	"tags":            true,
	"thumbnail_url":   true,
	"remote":          true,
	"users":           true,
	"created":         true,
	"remote_modified": true,
	"owner":           true,
	"editors":         true,
	"counters":        true,
}

var searchUpdateFields = map[string]bool{
	"filename":        true,
	"filetype":        true,
	"content":         true,
	"tags":            true,
	"thumbnail_url":   true,
	"users":           true,
	"remote_modified": true,
	"owner":           true,
	"editors":         true,
	"counters":        true,
	"forceSync":       true,
}

const DELIMITER = "|"

func Map(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error) {

	if input.Operation == "i" {

		output, err = ProcessDocument(input)

	} else {

		if SearchFieldsUpdated(input.UpdateDescription) {

			output, err = ProcessDocument(input)

		} else {
			output = &monstachemap.MapperPluginOutput{
				Skip: true,
			}
		}
	}

	return output, err
}

func ProcessDocument(input *monstachemap.MapperPluginInput) (output *monstachemap.MapperPluginOutput, err error) {

	//start := time.Now()

	document := input.Document
	assetId := document["_id"].(primitive.ObjectID).Hex()

	defer func() {
		if recoveredError := recover(); recoveredError != nil {
			err = fmt.Errorf("Syncing asset with ID %s failed with an unknow error: %s",
				assetId, recoveredError)
		}
	}()

	output = &monstachemap.MapperPluginOutput{
		Routing:  GetRouting(document),
		Document: document,
	}

	PruneDocument(document)
	TransformDocument(document)

	var wg sync.WaitGroup
	lookupErrors := make(chan string, 3)
	documentFragments := make(chan map[string]interface{}, 3)

	wg.Add(1)
	go AddFileTypeFields(input, document, &wg, documentFragments, lookupErrors)
	wg.Add(1)
	go AddTagNames(input, document, &wg, documentFragments, lookupErrors)
	wg.Add(1)
	go AddUserDetails(input, document, &wg, documentFragments, lookupErrors)

	wg.Wait()
	close(lookupErrors)
	close(documentFragments)

	var errorMessages = ""

	for errorMessage := range lookupErrors {
		errorMessages += errorMessage
	}

	if len(errorMessages) > 0 {
		err = fmt.Errorf("Syncing asset with ID %s failed with the following errors: %s",
			assetId, errorMessages)
	} else {
		for documentFragment := range documentFragments {
			for key, value := range documentFragment {
				document[key] = value
			}
		}
	}

	//elapsed := time.Since(start)
	//infoLog.Printf("Duration: %s", elapsed)

	return output, err
}

func SearchFieldsUpdated(updateDescription map[string]interface{}) (searchFieldsUpdated bool) {

	if updatedFields, ok := updateDescription["updatedFields"]; ok {

		for updatedField := range updatedFields.(map[string]interface{}) {

			if searchUpdateFields[updatedField] {
				return true
			}
		}

		return false

	} else {
		return true
	}
}

func GetRouting(document map[string]interface{}) (routing string) {

	return document["org"].(primitive.ObjectID).Hex()
}

func PruneDocument(document map[string]interface{}) {

	for fieldName := range document {

		if !assetFields[fieldName] {
			delete(document, fieldName)
		}
	}
}

func TransformDocument(document map[string]interface{}) {

	document["id"] = document["_id"].(primitive.ObjectID).Hex()
	delete(document, "_id")

	document["org"] = document["org"].(primitive.ObjectID).Hex()

	if remote, ok := document["remote"]; ok {
		if source, ok := remote.(map[string]interface{})["source"]; ok {
			document["remote_source"] = source.(string)
			delete(document, "remote")
		}
	}
}

func AddFileTypeFields(input *monstachemap.MapperPluginInput, document map[string]interface{}, wg *sync.WaitGroup,
	documentFragments chan map[string]interface{}, lookupErrors chan string) {

	defer wg.Done()

	assetId := document["id"].(string)
	filetypeId := document["filetype"].(primitive.ObjectID)
	documentFragment := make(map[string]interface{})

	filetype, err := LoadFiletype(input, assetId, filetypeId)

	if err == nil {

		documentFragment["filetype"] = filetype

		filetype["id"] = filetype["_id"]
		delete(filetype, "_id")

		categoryId := filetype["category"].(primitive.ObjectID)

		category, categoryErr := LoadCategory(input, filetypeId.Hex(), categoryId)

		if categoryErr == nil {

			filetype["category"] = category["name"].(string)

			filetypeTypeAhead := filetypeId.Hex() + DELIMITER
			filetypeTypeAhead += filetype["extension"].(string) + DELIMITER
			filetypeTypeAhead += category["name"].(string)

			documentFragment["filetype_type_ahead"] = filetypeTypeAhead

			documentFragments <- documentFragment

		} else {
			lookupErrors <- categoryErr.Error()
		}
	} else {
		lookupErrors <- err.Error()
	}
}

func LoadFiletype(input *monstachemap.MapperPluginInput, assetId string,
	filetypeId primitive.ObjectID) (filetype bson.M, err error) {

	client := input.MongoClient
	database := input.Database

	filetypeFields := bson.D{
		{"_id", 1},
		{"extension", 1},
		{"category", 1},
	}

	lookupError := client.Database(database).Collection("file_type").
		FindOne(context.Background(), bson.M{"_id": filetypeId},
			options.FindOne().SetProjection(filetypeFields)).Decode(&filetype)

	if lookupError != nil {
		return nil, fmt.Errorf("Failed to lookup filetype %s for asset %s: %s",
			filetypeId.Hex(), assetId, lookupError)
	}

	return
}

func LoadCategory(input *monstachemap.MapperPluginInput, filetypeId string,
	categoryId primitive.ObjectID) (category bson.M, err error) {

	client := input.MongoClient
	database := input.Database

	categoryFields := bson.D{
		{"_id", 0},
		{"name", 1},
	}

	lookupError := client.Database(database).Collection("file_category").
		FindOne(context.Background(), bson.M{"_id": categoryId},
			options.FindOne().SetProjection(categoryFields)).Decode(&category)

	if lookupError != nil {
		return nil, fmt.Errorf("Failed to lookup category %s for filetype %s: %s",
			categoryId.Hex(), filetypeId, lookupError)
	}

	return
}

func AddTagNames(input *monstachemap.MapperPluginInput, document map[string]interface{}, wg *sync.WaitGroup,
	documentFragments chan map[string]interface{}, lookupErrors chan string) {

	defer wg.Done()

	assetId := document["id"].(string)
	tags := document["tags"].([]interface{})
	tagIds := make([]primitive.ObjectID, len(tags))
	documentFragment := make(map[string]interface{})

	for index, tagId := range tags {
		tagIds[index] = tagId.(primitive.ObjectID)
	}

	tagNames, err := GetTags(input, assetId, tagIds)

	if err == nil {
		documentFragment["tags"] = tagNames
		documentFragments <- documentFragment
	} else {
		lookupErrors <- err.Error()
	}
}

func GetTags(input *monstachemap.MapperPluginInput, assetId string, tagIds []primitive.ObjectID) (tags []string, err error) {

	client := input.MongoClient
	database := input.Database

	tags = make([]string, len(tagIds))

	tagsFilter := bson.M{
		"_id": bson.M{
			"$in": tagIds,
		},
	}

	tagFields := bson.D{
		{"_id", 1},
		{"name", 1},
	}

	cur, lookupError := client.Database(database).Collection("tag").
		Find(context.Background(), tagsFilter,
			options.Find().SetProjection(tagFields))

	if lookupError != nil {
		return nil, fmt.Errorf("Failed to retrieve tags for asset %s: %s",
			assetId, lookupError)
	}

	defer cur.Close(context.Background())

	index := 0

	tagIdsRetrieved := make([]string, 0, len(tagIds))

	for cur.Next(context.Background()) {

		tag := struct {
			Id   string `bson:"_id"`
			Name string `bson:"name"`
		}{}

		decodeErr := cur.Decode(&tag)

		if decodeErr != nil {
			return nil, fmt.Errorf("Failed to decode tag for asset %s: %s",
				assetId, decodeErr)
		}

		tagIdsRetrieved = append(tagIdsRetrieved, tag.Id)

		tags[index] = strings.ToLower(tag.Name)
		index++
	}

	if cursorErr := cur.Err(); cursorErr != nil {
		return nil, fmt.Errorf("Failed while iterating through tags for asset %s: %s",
			assetId, cursorErr)
	}

	if len(tagIdsRetrieved) < len(tagIds) {
		warnLog.Printf("Attempted to retrieve %d tags for asset %s but only retrieved %d with IDs %s",
			len(tagIds), assetId, len(tagIdsRetrieved), tagIdsRetrieved)
	}

	return
}

func AddUserDetails(input *monstachemap.MapperPluginInput, document map[string]interface{}, wg *sync.WaitGroup,
	documentFragments chan map[string]interface{}, lookupErrors chan string) {

	defer wg.Done()

	assetId := document["id"].(string)
	emails := document["users"].([]interface{})
	emailIds := make([]string, len(emails))
	usersTypeAhead := make([]string, len(emails))
	documentFragment := make(map[string]interface{})

	for index, emailId := range emails {
		emailIds[index] = emailId.(string)
	}

	users, err := GetUsers(input, assetId, emailIds)

	if err == nil {

		documentFragment["users"] = users

		for index, user := range users {

			usersTypeAhead[index] = user["name"] + DELIMITER + user["email"]
		}

		documentFragment["users_type_ahead"] = usersTypeAhead
		documentFragments <- documentFragment
	} else {
		lookupErrors <- err.Error()
	}
}

func GetUsers(input *monstachemap.MapperPluginInput, assetId string, emails []string) (users []map[string]string, err error) {

	client := input.MongoClient
	database := input.Database

	users = make([]map[string]string, len(emails))

	usersFilter := bson.M{
		"_id": bson.M{
			"$in": emails,
		},
	}

	userFields := bson.D{
		{"email", "$_id"},
		{"name", 1},
	}

	cur, lookupError := client.Database(database).Collection("user").
		Find(context.Background(), usersFilter,
			options.Find().SetProjection(userFields))

	if lookupError != nil {
		return nil, fmt.Errorf("Failed to retrieve users for asset %s: %s",
			assetId, lookupError)
	}

	defer cur.Close(context.Background())

	index := 0

	usersRetrieved := make([]string, 0, len(emails))

	for cur.Next(context.Background()) {

		user := struct {
			Email string `bson:"_id"`
			Name  string `bson:"name"`
		}{}

		decodeErr := cur.Decode(&user)

		if decodeErr != nil {
			return nil, fmt.Errorf("Failed to decode user for asset %s: %s",
				assetId, decodeErr)
		}

		usersRetrieved = append(usersRetrieved, user.Email)

		users[index] = map[string]string{
			"email": user.Email,
			"name":  user.Name,
		}

		index++
	}

	if cursorErr := cur.Err(); cursorErr != nil {
		return nil, fmt.Errorf("Failed while iterating through users for asset %s: %s",
			assetId, cursorErr)
	}

	if len(usersRetrieved) < len(emails) {
		return nil, fmt.Errorf("Attempted to retrieve %d users for asset %s but only retrieved %d with IDs %s",
			len(emails), assetId, len(usersRetrieved), usersRetrieved)
	}

	return
}
