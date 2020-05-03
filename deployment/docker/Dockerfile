# docker build -f deployment/docker/Dockerfile -t tweet-loader-ranker .
FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./tweet_main.py" ]