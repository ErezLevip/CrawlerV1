package main

import (
	"log"
	"net/http"
	"golang.org/x/net/html"
	"encoding/json"
	"bytes"
	"runtime"
	"CrawlerV1/Crawler/Engine"
	"CrawlerV1/Crawler/DbHandler"
	"CrawlerV1/Crawler/GlobalTypes"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	engine := Engine.CrawlerEngine{}.Make(Engine.CrawlerConfiguration{
		MaxBulkSize:     1000,
		BaseUrl:         "http://imdb.com/title/tt00",
		NumberOfWorkers: 1000,
		MinIndex:        11001,
		MongoConfig: DbHandler.MongoConfiguration{
			ConnectionString: "conStr",
		},
	}, crawl)

	engine.Start()
}

func crawl(s string) GlobalTypes.Data {
	r, err := http.Get(s) //send http get request
	if (err != nil) {
		log.Println(err.Error())
	} else {
		tokenizer := html.NewTokenizer(r.Body) //initialize Tokenizer
		movie := GetMovieData(tokenizer)       // get the movie after processing

		defer r.Body.Close() //close the reader
		//serialize the movie data to bytes
		jdata, err := json.Marshal(movie)
		if (err != nil) {
			log.Println(err.Error())
		} else {
			return GlobalTypes.Data{
				Value: bytes.NewReader(jdata), // insert the bytes to a reader and create Data Type
				Key:   movie.Name,
			}
		}
	}
	return GlobalTypes.Data{}
}

func GetMovieData(tokenizer *html.Tokenizer) (movie *GlobalTypes.Movie) {
	movie = &GlobalTypes.Movie{// create a pointer to a default movie type
		Actors: make([]string, 0),
	}
	for {
		tagToken := tokenizer.Next() // run for each tag
		switch {
		case tagToken == html.ErrorToken: //stop on error
			return
		case tagToken == html.StartTagToken:
			t := tokenizer.Token()

			if (t.Data == "h1") { // search for h1 tags
				for _, att := range t.Attr {
					if (att.Key == "itemprop" && att.Val == "name") {
						tokenizer.Next()
						movie.Name = tokenizer.Token().Data //set the movie data
					}
				}
			} else if (t.Data == "span") { // search for span tags
				for _, att := range t.Attr {
					if (att.Key == "itemprop" && att.Val == "name") {
						tokenizer.Next()
						actor := tokenizer.Token().Data
						movie.Actors = append(movie.Actors, actor) //append Actors data
					}
				}
			}
		}
	}
}
