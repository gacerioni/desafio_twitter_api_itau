import pymongo
import tweepy
import configparser
from operator import itemgetter


#LOAD PARAMS FROM CONFIG FILE
config = configparser.ConfigParser()
config.read('config.ini')

# TWITTER_CREDENTIALS
C_KEY = config['TWITTER_CREDENTIALS']['C_KEY']
C_SECRET = config['TWITTER_CREDENTIALS']['C_SECRET']
A_TOKEN = config['TWITTER_CREDENTIALS']['A_TOKEN']
A_TOKEN_SECRET = config['TWITTER_CREDENTIALS']['A_TOKEN_SECRET']

#MONGODB_PARAMS
MONGO_SERVER = config['MONGODB_PARAMS']['MONGO_SERVER']
MONGO_DB = config['MONGODB_PARAMS']['MONGO_DB']
MONGO_TWEETS_COL = config['MONGODB_PARAMS']['MONGO_TWEETS_COL']
MONGO_RANK_COL = config['MONGODB_PARAMS']['MONGO_RANK_COL']
MONGO_USER = config['MONGODB_PARAMS']['MONGO_USER']
MONGO_PWD = config['MONGODB_PARAMS']['MONGO_PWD']


def local_twitter_api_auth(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    return (api)


def get_tweeted_user_ids(mongo_server, mongo_db, mongo_col, mongo_user, mongo_pwd):
    user_ids = []
    myclient = pymongo.MongoClient(mongo_server, username=mongo_user, password=mongo_pwd)
    mydb = myclient[mongo_db]
    mycol = mydb[mongo_col]

    cursor = mycol.find({})
    for document in cursor:
        user_id = document['user']
        user_ids.append(user_id)

    dedup_list = list(dict.fromkeys(user_ids))
    return(dedup_list)


def delete_old_rank(mongo_server, mongo_db, mongo_col, mongo_user, mongo_pwd):
    myclient = pymongo.MongoClient(mongo_server, username=mongo_user, password=mongo_pwd)
    mydb = myclient[mongo_db]
    mycol = mydb[mongo_col]

    x = mycol.delete_many({})

    return(x.deleted_count, " documents deleted.")


def insert_user_rank(top_user_rank, mongo_server, mongo_db, mongo_col, mongo_user, mongo_pwd):
    myclient = pymongo.MongoClient(mongo_server, username=mongo_user, password=mongo_pwd)
    mydb = myclient[mongo_db]
    mycol = mydb[mongo_col]

    x = mycol.insert_many(top_user_rank)

    return (x.inserted_ids)


def get_user_info(api, user_identification):
    user_raw = api.get_user(user_id=user_identification)
    user_raw_json = user_raw._json
    user_filtered = {'name': user_raw_json['name'], 'location': user_raw_json['location'], 'followers_count': user_raw_json['followers_count']}
    return(user_filtered)


def get_filtered_user_list(api, user_id_list):
    filtered_user_list = []
    for user in user_id_list:
        filtered_user = get_user_info(api, user)
        filtered_user_list.append(filtered_user)
    return(filtered_user_list)


def create_rank_by_followers(user_filtered_list, topn):
    sorted_list = sorted(user_filtered_list, key=itemgetter('followers_count'), reverse=True)
    top_list = sorted_list[:topn]
    return(top_list)


def main():

    # 1-) Get a dedupped list with all users that tweeted somehing.
    users = get_tweeted_user_ids(MONGO_SERVER, MONGO_DB, MONGO_TWEETS_COL, MONGO_USER, MONGO_PWD)

    # 2-) A very simple way to auth with my twitter credentials
    api_auth = local_twitter_api_auth(C_KEY, C_SECRET, A_TOKEN, A_TOKEN_SECRET)

    # 3-) Get a ready to go user list from tweeter with the info we need
    filtered_user_list = get_filtered_user_list(api_auth, users)

    # 4-) Get the top 5 users considering the followers
    top_rank = create_rank_by_followers(filtered_user_list, 5)

    # 5-) Clean the last Ranking
    print(delete_old_rank(MONGO_SERVER, MONGO_DB, MONGO_RANK_COL, MONGO_USER, MONGO_PWD))

    # 6-) Finally, add the rank in the Collection!
    insert_user_rank(top_rank, MONGO_SERVER, MONGO_DB, MONGO_RANK_COL, MONGO_USER, MONGO_PWD)


if __name__ == "__main__":
    main()

