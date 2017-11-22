package DbHandler

import (
	"io"
	"fmt"
	"CrawlerV1/Crawler/GlobalTypes"
	"bytes"
)

type DbHandler interface {
	Insert(collection string, data GlobalTypes.Data)
}

type MongoDbHandler struct {
	Config MongoConfiguration
}

type MongoConfiguration struct {
	ConnectionString string
	Database         string
}

func (m MongoDbHandler) Make(config MongoConfiguration) DbHandler {
	return &MongoDbHandler{
		Config: config,
	}
}

// simulate insert of the data to mongodb, might read outside or pass a key value in order to pass the key before the insert
func (m *MongoDbHandler) Insert(collection string, data GlobalTypes.Data) {

	if (data.Value == nil) {
		return
	}

	buff := make([]byte, bytes.MinRead)   //512 bytes buffer
	result := make([]byte, 0) //the total slice of bytes
	for {
		_, err := data.Value.Read(buff)
		if (err == io.EOF) {
			break
		}
		result = append(result, buff...) // combine the new buffer slice and the total
	}

	fmt.Println(data.Key, "processed successfully")
}
