# docker build -t python-tweet-loader .
# docker run -it --rm --name python-tweet-loader python-tweet-loader
FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./tweet_loader.py" ]