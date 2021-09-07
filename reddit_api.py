import urllib
import requests
import pandas as pd
import json
from urllib.request import urlopen
import time
import text_analytics


def get_comment_posts(post_id, client_text_analytics=None):
    scrapping = True
    comments = None
    while scrapping:
        try:
            comments = json.loads(urlopen("https://www.reddit.com/comments/" + post_id + ".json").read())
            scrapping = False
        except urllib.error.HTTPError:
            print("Limit request reached: Waiting 1 min.")
            time.sleep(90)  # Limited to 60 requests per minute
            print("Retry request.")

    post = comments[0]["data"]["children"][0]["data"]
    df = pd.DataFrame()  # initialize dataframe
    if "i.redd.it" in post['url']:
        url = "https://www.reddit.com" + post["permalink"]
        image_url = post['url']
    else:
        url = post['url']
        image_url = ""
    if client_text_analytics is not None:
        post_analytics = text_analytics.sentiment_analysis(client_text_analytics, post["selftext"]).iloc[0]
        post_sentiment = post_analytics["sentence_sentiment"]
        post_confidence_positive = post_analytics["sentence_confidence_positive"]
        post_confidence_neutral = post_analytics["sentence_confidence_neutral"]
        post_confidence_negative = post_analytics["sentence_confidence_negative"]
    else:
        post_sentiment = None
        post_confidence_positive = None
        post_confidence_neutral = None
        post_confidence_negative = None

    for comment in comments[1]["data"]["children"]:
        try:
            body_data = comment["data"]['body']
        except KeyError:
            body_data = ""
        if client_text_analytics is not None:
            comment_analytics = text_analytics.sentiment_analysis(client_text_analytics,
                                                                  body_data).iloc[0]
            comment_sentiment = comment_analytics["sentence_sentiment"]
            comment_confidence_positive = comment_analytics["sentence_confidence_positive"]
            comment_confidence_neutral = comment_analytics["sentence_confidence_neutral"]
            comment_confidence_negative = comment_analytics["sentence_confidence_negative"]
        else:
            comment_sentiment = None
            comment_confidence_positive = None
            comment_confidence_neutral = None
            comment_confidence_negative = None
        try:
            df = df.append({
                'subreddit': post['subreddit'],
                'title': post['title'],
                'selftext': post['selftext'],
                'url': url,
                "image_url": image_url,
                'id': post['id'],
                'ups': post['ups'],
                'created': post['created'],
                'downs': post['downs'],
                'upvote_ratio': post['upvote_ratio'],
                'body': comment['data']['body'],
                'id_comment': comment['data']['id'],
                'ups_comment': comment['data']['ups'],
                'downs_comment': comment['data']['downs'],
                'score_comment': comment['data']['score'],
                'permalink_comment': comment['data']['permalink'],
                'post_sentiment': post_sentiment,
                'post_confidence_positive': post_confidence_positive,
                'post_confidence_neutral': post_confidence_neutral,
                'post_confidence_negative': post_confidence_negative,
                'comment_sentiment': comment_sentiment,
                'comment_confidence_positive': comment_confidence_positive,
                'comment_confidence_neutral': comment_confidence_neutral,
                'comment_confidence_negative': comment_confidence_negative

            }, ignore_index=True)
        except KeyError:
            print("ERROR on " + url + " comment: " + comment['data']['id'])
            print("Permalink: https://www.reddit.com" + comment["data"]["permalink"])

    return df


# params mode = new hot or top
def get_posts(headers, subreddit="magicTCG", mode="hot", client_text_analytics=None):
    request_results = requests.get("https://oauth.reddit.com/r/" + subreddit + "/" + mode + "",
                                   headers=headers)

    df = pd.DataFrame()  # initialize dataframe

    # loop through each post retrieved from GET request
    for post in request_results.json()['data']['children']:
        # append relevant data to dataframe
        df = df.append(get_comment_posts(post['data']['id'], client_text_analytics))
    return df
