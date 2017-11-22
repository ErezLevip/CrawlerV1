package Engine

import (
	"CrawlerV1/Crawler/DbHandler"
	"CrawlerV1/Crawler/GlobalTypes"
	"log"
)

type Engine interface {
	Start()
}


type CrawlerEngine struct {
	CrawlerConfig CrawlerConfiguration
	ProcessingLogic func(string) GlobalTypes.Data
}

type CrawlerConfiguration struct {
	NumberOfWorkers int
	MongoConfig     DbHandler.MongoConfiguration
	BaseUrl         string
	MinIndex        int
	MaxBulkSize     int
}

func (e CrawlerEngine) Make(configuration CrawlerConfiguration, logic func(string) GlobalTypes.Data) Engine {
	return &CrawlerEngine{
		CrawlerConfig: configuration,
		ProcessingLogic: logic,
	}
}

func (e *CrawlerEngine) Start() {
	chunks := GenerateUrlBulks(e.CrawlerConfig) //generate a chunk of urls for each go routine
	log.Println("Chunks!!",chunks)
	for _, chunk := range chunks {
		results := make(chan GlobalTypes.Data) //will collect all the results from the current chunk
		go func() {
			for _, url := range chunk {
				r := e.ProcessingLogic(url) // execute the logic that was given
				results <- r // insert the result into the channel
			}
			close(results)
		}()

		//insert the results to the mock (mongodb) database
		for r := range results {
			mongoHandler := DbHandler.MongoDbHandler{}.Make(e.CrawlerConfig.MongoConfig)
			mongoHandler.Insert("Movies",r) // insert each result to Movies collection. this method will close the connection on defer
		}
	}
}
