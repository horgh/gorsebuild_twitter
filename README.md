This is a utility to create an RSS feed XML based on a postgres database
containing tweets from twitter.

The database gets populated using one of my other utilities: twitter_poll.tcl
which is part of my twitter-tcl repository.

The reason I want to do this is to consolidate the frontends for viewing
my RSS feed items and viewing my tweet feeds. Right now I have one website
for viewing RSS feed items (gorse), and one for viewing the tweets (which
I have not maintained very much and is more clunky to use - and I don't want
to really be working on two that are quite similar.

So using this I will be able to point my RSS reader at an RSS feed generated
from my tweets and do away with my 'twitreader' interface!
