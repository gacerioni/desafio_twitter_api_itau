import tweet_loader
import most_followers_ranker

import logging

# Logging config
logging.basicConfig(format='%(asctime)s --- %(levelname)s --- %(message)s', level=logging.INFO)


def main():
    tweet_loader.main()
    most_followers_ranker.main()


if __name__ == "__main__":
    logging.info("Starting the Tweet Main Engine!")
    main()
    logging.info("Full process has finished!")
