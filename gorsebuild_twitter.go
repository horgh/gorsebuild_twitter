//
// Generate an RSS feed from a PostgreSQL database containing tweets.
//
// The tweet database is the one populated by my twitter-tcl twitter_poll
// program.
//
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"summercat.com/config"
	"summercat.com/gorse/gorselib"
)

// FeedURI is the URI set on the RSS feed's channel element's link element.
// It need not be a real URI but should be unique.
var FeedURI = "https://leviathan.summercat.com/tweets/"

// Tweet describe a tweet pulled from the database.
type Tweet struct {
	Nick    string
	Text    string
	Time    time.Time
	TweetID int64
}

// MyConfig holds configuration values.
type MyConfig struct {
	DBUser string
	DBPass string
	DBName string
	DBHost string
	// the number of recent tweets to put in the xml.
	NumTweets uint64
}

// connectToDB opens a new connection to the database.
func connectToDB(name string, user string, pass string, host string) (*sql.DB,
	error) {
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
	db, err := connectToDB(config.DBName, config.DBUser, config.DBPass,
		config.DBHost)
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
		err = rows.Scan(&tweet.Nick, &tweet.Text, &tweet.Time, &tweet.TweetID)
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
func createStatusURL(screenName string, tweetID int64) string {
	return fmt.Sprintf("https://twitter.com/%s/status/%d",
		screenName, tweetID)
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
	// TODO: we could run validation on each config item... but then again,
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
	rss.Uri = FeedURI
	rss.Description = "Twitreader tweets"
	rss.LastUpdateTime = time.Now()

	// build rss items.
	for _, tweet := range tweets {
		item := gorselib.RssItem{
			Title:           fmt.Sprintf("%s (#%d)", tweet.Nick, tweet.TweetID),
			Uri:             createStatusURL(tweet.Nick, tweet.TweetID),
			Description:     tweet.Text,
			PublicationDate: tweet.Time,
		}
		rss.Items = append(rss.Items, item)
	}

	err = gorselib.WriteFeedXML(&rss, *outputFile)
	if err != nil {
		log.Printf("Failed to write XML: %s", err.Error())
		os.Exit(1)
	}
}
