package Engine

import (
	"strconv"
)

func GenerateUrlBulks(config CrawlerConfiguration) [][]string {
	bulks := make([][]string, 0)
	//run for each go routine that will be created
	for i := 0; i < config.NumberOfWorkers; i++ {
		// create the bulk
		bulk := make([]string, 0)
		// run for each url
		for j := 0; j < config.MaxBulkSize; j++ {
			//add the url to the bulk of each go routine
			bulk = append(bulk, config.BaseUrl+strconv.Itoa(config.MinIndex+j))
		}
		bulks = append(bulks,bulk) //append the new bulk
	}
	return bulks
}
