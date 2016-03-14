/*
 * generate an rss feed using a postgres database containing tweets
 */

package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"summercat.com/config"
	"summercat.com/gorse/gorselib"
	"time"
)

// URI prefix to generate unique URIs.
// this is not to be a real URI (though I suppose it could link to the
// tweets on twitter) but only to provide uniqueness.
var UriPrefix = "https://leviathan.summercat.com/tweets/"

// describe a tweet from the database.
type Tweet struct {
	Nick    string
	Text    string
	Time    time.Time
	TweetId int64
}

// configuration items.
type MyConfig struct {
	DbUser string
	DbPass string
	DbName string
	DbHost string
	// the number of recent tweets to put in the xml.
	NumTweets uint64
}

// connectToDb opens a new connection to the database.
func connectToDb(name string, user string, pass string, host string) (*sql.DB,
	error) {
	// connect to the database.
	dsn := fmt.Sprintf("user=%s password=%s dbname=%s host=%s", user, pass, name,
		host)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Failed to connect to the database: %s", err.Error())
		return nil, err
	}
	return db, nil
}

// getTweets retrieves tweets from a database.
func getTweets(config *MyConfig) ([]Tweet, error) {
	// get a connection.
	db, err := connectToDb(config.DbName, config.DbUser, config.DbPass,
		config.DbHost)
	if err != nil {
		return nil, err
	}

	// get most recent tweets.
	sql := `
SELECT nick, text, time, tweet_id
FROM tweet
ORDER BY time DESC
LIMIT $1
`
	rows, err := db.Query(sql, config.NumTweets)
	if err != nil {
		log.Printf("Query failure: %s", err.Error())
		return nil, err
	}

	var tweets []Tweet
	for rows.Next() {
		tweet := Tweet{}
		err = rows.Scan(&tweet.Nick, &tweet.Text, &tweet.Time, &tweet.TweetId)
		if err != nil {
			log.Printf("Failed to scan row: %s", err.Error())
			// TODO: is there anything to clean up?
			return nil, err
		}
		tweets = append(tweets, tweet)
	}
	// I'm adding a close because I see 'unexpected EOF on client connection'
	// in postgresql logs from this. with a close it goes away!
	err = db.Close()
	if err != nil {
		log.Printf("Failed to close database connection: %s", err.Error())
		return nil, err
	}
	return tweets, nil
}

// create a URL to the status.
// apparently this URL is not in the tweet status payload.
// form:
// https://twitter.com/<screenname>/status/<tweetid>
func createStatusURL(screenName string, tweetId int64) string {
	return fmt.Sprintf("https://twitter.com/%s/status/%d",
		screenName, tweetId)
}

// main is the program entry point.
func main() {
	log.SetFlags(log.Ltime | log.Llongfile)

	// command line arguments.
	outputFile := flag.String("output-file", "", "Output XML file to write.")
	configFile := flag.String("config-file", "", "Config file")
	flag.Parse()
	if len(*outputFile) == 0 || len(*configFile) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// load up the config.
	var settings MyConfig
	err := config.GetConfig(*configFile, &settings)
	if err != nil {
		log.Printf("Failed to retrieve config: %s", err.Error())
		os.Exit(1)
	}
	// TODO: we could run validation on each config item... but thn again,
	//   we can just try to connect to the database!

	// reduce some library logging.
	gorselib.SetQuiet(true)

	// retrieve recent tweets.
	tweets, err := getTweets(&settings)
	if err != nil {
		log.Printf("Failed to retrieve tweets: %s", err.Error())
		os.Exit(1)
	}

	// set up the feed's information.
	rss := gorselib.RssFeed{}
	rss.Name = "Twitreader"
	rss.Uri = UriPrefix
	rss.Description = "Twitreader tweets"
	rss.LastUpdateTime = time.Now()

	// build rss items.
	for _, tweet := range tweets {
		item := gorselib.RssItem{
			Title:           fmt.Sprintf("%s (#%d)", tweet.Nick, tweet.TweetId),
			Uri:             createStatusURL(tweet.Nick, tweet.TweetId),
			Description:     tweet.Text,
			PublicationDate: tweet.Time,
		}
		rss.Items = append(rss.Items, item)
	}

	// generate xml.
	err = gorselib.WriteFeedXml(&rss, *outputFile)
	if err != nil {
		log.Printf("Failed to write XML: %s", err.Error())
		os.Exit(1)
	}
}
