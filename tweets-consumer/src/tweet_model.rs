use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum StreamMessage {
    Tweet(Tweet),
    Other(serde::de::IgnoredAny),
}

#[derive(Debug, Deserialize)]
pub struct Tweet {
    pub id: u64,
    pub user: User,
    pub text: String,
    pub entities: Option<Entities>,
    //source: String,
    pub lang: String,
    pub possibly_sensitive: bool,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub screen_name: String,
    pub location: String,
}

#[derive(Debug, Deserialize)]
pub struct Entities {
    pub hashtags: Vec<Hashtag>,
}

#[derive(Debug, Deserialize)]
pub struct Hashtag {
    pub text: String,
}
