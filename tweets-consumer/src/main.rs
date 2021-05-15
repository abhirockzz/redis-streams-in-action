use redis::Commands;
use std::collections::btree_map::BTreeMap;
use std::sync::Arc;

use futures::prelude::*;
use twitter_stream::{Token, TwitterStream};

use std::env;
use std::sync::Mutex;

mod tweet_model;
use tweet_model as model;

#[tokio::main]
async fn main() {
    let conn = connect_redis();
    let c = Arc::new(Mutex::new(conn));

    let token = twitter_token();

    TwitterStream::sample(&token)
        .try_flatten_stream()
        .try_for_each(|json| {
            let msg: model::StreamMessage =
                serde_json::from_str(&json).expect("failed to convert tweet JSON to struct");
            process(msg, c.clone());
            future::ok(())
        })
        .await
        .expect("error connecting to Twitter stream!");
}

fn process(msg: model::StreamMessage, conn: Arc<Mutex<redis::Connection>>) {
    match msg {
        model::StreamMessage::Tweet(tweet) => {
            if tweet.lang.eq("en") && !tweet.possibly_sensitive {
                println!("tweet {:?}", tweet);

                let mut stream_entry: BTreeMap<String, String> = BTreeMap::new();
                stream_entry.insert("id".to_string(), tweet.id.to_string());
                stream_entry.insert("user".to_string(), tweet.user.screen_name);
                stream_entry.insert("text".to_string(), tweet.text);
                stream_entry.insert("location".to_string(), tweet.user.location);

                //converting a Vec<Hashtag> into Vec<String>
                let hashtag_texts: Vec<String> = tweet
                    .entities
                    .expect("no Entities in tweet")
                    .hashtags
                    .into_iter()
                    .map(|h| h.text)
                    .collect();

                //converting Vec<String> to a single CSV String. this is to match the Redisearch representation
                let hashtags = hashtag_texts.join(",");
                stream_entry.insert("hashtags".to_string(), hashtags);

                let stream_entry_id: String = conn
                    .try_lock()
                    .expect("failed to get a lock on redis connection")
                    .xadd_map("tweets_stream", "*", stream_entry)
                    .expect("XADD failed");

                println!(
                    "***** XADD for tweet successful. Stream ID {} *****",
                    stream_entry_id
                );
            }
        }
        model::StreamMessage::Other(_) => {
            //println!("not a tweet. ignored");
        }
    }
}

fn connect_redis() -> redis::Connection {
    println!("Connecting to Redis");
    let redis_host_name =
        env::var("REDIS_HOSTNAME").expect("missing environment variable REDIS_HOSTNAME");
    let redis_password = env::var("REDIS_PASSWORD").unwrap_or_default();

    //if Redis server needs secure connection
    let uri_scheme = match env::var("IS_TLS") {
        Ok(_) => "rediss",
        Err(_) => "redis",
    };

    let redis_conn_url = format!("{}://:{}@{}", uri_scheme, redis_password, redis_host_name);
    println!("redis_conn_url {}", redis_conn_url);

    let client = redis::Client::open(redis_conn_url).expect("check Redis connection URL");
    client.get_connection().expect("failed to connect to Redis")
}

fn twitter_token() -> twitter_stream::Token {
    let twitter_api_key =
        env::var("TWITTER_API_KEY").expect("missing environment variable TWITTER_API_KEY");

    let twitter_api_key_secret = env::var("TWITTER_API_KEY_SECRET")
        .expect("missing environment variable TWITTER_API_KEY_SECRET");

    let twitter_access_token = env::var("TWITTER_ACCESS_TOKEN")
        .expect("missing environment variable TWITTER_ACCESS_TOKEN");

    let twitter_access_token_secret = env::var("TWITTER_ACCESS_TOKEN_SECRET")
        .expect("missing environment variable TWITTER_ACCESS_TOKEN_SECRET");

    Token::from_parts(
        twitter_api_key,
        twitter_api_key_secret,
        twitter_access_token,
        twitter_access_token_secret,
    )
}
