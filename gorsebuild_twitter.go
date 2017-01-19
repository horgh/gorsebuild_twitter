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

	"github.com/horgh/config"
	"github.com/horgh/rss"
	_ "github.com/lib/pq"
)

// FeedURI is the URI set on the RSS feed's channel element's link element. It
// need not be a real URI but should be unique.
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
	// The number of recent tweets to put in the XML.
	NumTweets uint64
}

// connectToDB opens a new connection to the database.
func connectToDB(name string, user string, pass string, host string) (*sql.DB,
	error) {
	dsn := fmt.Sprintf("user=%s password=%s dbname=%s host=%s", user, pass, name,
		host)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %s", err)
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

	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("Database close: %s", err)
		}
	}()

	// get most recent tweets.
	sql := `
SELECT nick, text, time, tweet_id
FROM tweet
ORDER BY time DESC
LIMIT $1
`
	rows, err := db.Query(sql, config.NumTweets)
	if err != nil {
		return nil, fmt.Errorf("query failure: %s", err)
	}

	var tweets []Tweet
	for rows.Next() {
		tweet := Tweet{}

		err = rows.Scan(&tweet.Nick, &tweet.Text, &tweet.Time, &tweet.TweetID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %s", err)
		}

		tweets = append(tweets, tweet)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("failure fetching rows: %s", err)
	}

	return tweets, nil
}

// Create a URL to the status.
//
// Apparently this URL is not in the tweet status payload.
//
// Form: https://twitter.com/<screenname>/status/<tweetid>
func createStatusURL(screenName string, tweetID int64) string {
	return fmt.Sprintf("https://twitter.com/%s/status/%d", screenName, tweetID)
}

func main() {
	log.SetFlags(log.Ltime | log.Llongfile)

	outputFile := flag.String("output-file", "", "Output XML file to write.")
	configFile := flag.String("config-file", "", "Config file")

	flag.Parse()

	if len(*outputFile) == 0 || len(*configFile) == 0 {
		fmt.Println("You must provide an output file and a config file.")
		flag.PrintDefaults()
		os.Exit(1)
	}

	var settings MyConfig
	err := config.GetConfig(*configFile, &settings)
	if err != nil {
		log.Fatalf("Failed to retrieve config: %s", err)
	}

	// TODO: We could run validation on each config item.

	rss.SetVerbose(false)

	tweets, err := getTweets(&settings)
	if err != nil {
		log.Fatalf("Failed to retrieve tweets: %s", err)
	}

	feed := rss.Feed{
		Title:       "Twitreader",
		Link:        FeedURI,
		Description: "Twitreader tweets",
		PubDate:     time.Now(),
	}

	for _, tweet := range tweets {
		feed.Items = append(feed.Items, rss.Item{
			Title:       fmt.Sprintf("%s", tweet.Nick),
			Link:        createStatusURL(tweet.Nick, tweet.TweetID),
			Description: tweet.Text,
			PubDate:     tweet.Time,
		})
	}

	err = rss.WriteFeedXML(feed, *outputFile)
	if err != nil {
		log.Fatalf("Failed to write XML: %s", err)
	}
}
