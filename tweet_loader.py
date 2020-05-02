import pymongo
import tweepy
import configparser


#LOAD PARAMS FROM CONFIG FILE
config = configparser.ConfigParser()
config.read('config.ini')

# TWITTER_LOADER_PARAMS
HASHTAGS = config['TWITTER_LOADER_PARAMS']['HASHTAGS'].strip().replace(" ", "").split(",")
TWEET_LIMITS = int(config['TWITTER_LOADER_PARAMS']['TWEET_LIMITS'])

# TWITTER_CREDENTIALS
C_KEY = config['TWITTER_CREDENTIALS']['C_KEY']
C_SECRET = config['TWITTER_CREDENTIALS']['C_SECRET']
A_TOKEN = config['TWITTER_CREDENTIALS']['A_TOKEN']
A_TOKEN_SECRET = config['TWITTER_CREDENTIALS']['A_TOKEN_SECRET']

#MONGODB_PARAMS
MONGO_SERVER = config['MONGODB_PARAMS']['MONGO_SERVER']
MONGO_DB = config['MONGODB_PARAMS']['MONGO_DB']
MONGO_COL = config['MONGODB_PARAMS']['MONGO_COL']
MONGO_USER = config['MONGODB_PARAMS']['MONGO_USER']
MONGO_PWD = config['MONGODB_PARAMS']['MONGO_PWD']


def local_twitter_api_auth(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    return (api)


def get_recent_tweets_by_hashtag(api, hashtag, limits):
    raw_tweets = [raw._json for raw in tweepy.Cursor(api.search, q=hashtag, result_type="recent").items(limits)]
    # se precisar de json como string
    # json_as_str = [json.dumps(json_obj) for json_obj in raw_tweets]
    minimal_tweet_list = [{'created_at': raw_tw['created_at'], 'hashtag': hashtag, \
                           'user': raw_tw['user']['id'], 'lang': raw_tw['lang']} \
                          for raw_tw in raw_tweets]

    return (minimal_tweet_list)


def get_all_tweets(api, hashtags_list, limits):
    all_minimal_tweets = []

    for hashtag in hashtags_list:
        minimal_list = get_recent_tweets_by_hashtag(api, hashtag, limits)
        all_minimal_tweets.extend(minimal_list)

    return (all_minimal_tweets)


def insert_many_tweets(tweet_list, mongo_server, mongo_db, mongo_col, mongo_user, mongo_pwd):
    myclient = pymongo.MongoClient(mongo_server, username=mongo_user, password=mongo_pwd)
    mydb = myclient[mongo_db]
    mycol = mydb[mongo_col]

    x = mycol.insert_many(tweet_list)

    return (x.inserted_ids)


def main():
    # 1-) A very simple way to auth with my twitter credentials
    api_auth = local_twitter_api_auth(C_KEY, C_SECRET, A_TOKEN, A_TOKEN_SECRET)

    # 2-) Loading all formatted tweets, based on our desired hashtags
    minimal_tweet_list = get_all_tweets(api_auth, HASHTAGS, TWEET_LIMITS)

    # 3-) Indexing all those tweets!
    result = insert_many_tweets(minimal_tweet_list, MONGO_SERVER, MONGO_DB, MONGO_COL, MONGO_USER, MONGO_PWD)

    print(result)


if __name__ == "__main__":
    main()
