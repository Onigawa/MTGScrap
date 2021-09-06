import urllib

import yaml
import requests
import pandas as pd
import json
from urllib.request import urlopen
import time

config = yaml.safe_load(open("config.yaml", 'r'))

CLIENT_ID = config.get("client_id")
SECRET_KEY = config.get("secret_key")
auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_KEY)

# here we pass our login method (password), username, and password
data = {'grant_type': 'password',
        'username': 'Onigawa',
        'password': config.get("reddit_password")}

# setup our header info, which gives reddit a brief description of our app
headers = {'User-Agent': 'MyBot/0.0.1'}

# send our request for an OAuth token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

# convert response to JSON and pull access_token value
TOKEN = res.json()['access_token']

# add authorization to our headers dictionary
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

# while the token is valid (~2 hours) we just add headers=headers to our requests
requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)


def get_comment_posts(post_id):
    try:
        comments = json.loads(urlopen("https://www.reddit.com/comments/"+post_id+".json").read())
    except urllib.error.HTTPError:
        print("Limit request reached: Waiting 1 min.")
        time.sleep(90)  # Limited to 60 requests per minute
        comments = json.loads(urlopen("https://www.reddit.com/comments/" + post_id + ".json").read())

    post = comments[0]["data"]["children"][0]
    df = pd.DataFrame()  # initialize dataframe

    for comment in comments[1]["data"]["children"]:
        if str(post['data']['url']).lower().endswith(".jpg") or str(post['data']['url']).lower().endswith(".png"):
            body = ""
            id_comment = ""
            ups_comment = 0
            downs_comment = 0
            score_comment = 0
            permalink_comment = ""
        else:
            body = comment['data']['body']
            id_comment = comment['data']['id']
            ups_comment = comment['data']['ups']
            downs_comment = comment['data']['downs']
            score_comment = comment['data']['score']
            permalink_comment = comment['data']['permalink']
        try:
            df = df.append({
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'url': post['data']['url'],
                'id': post['data']['id'],
                'ups': post['data']['ups'],
                'created': post['data']['created'],
                'downs': post['data']['downs'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'body': body,
                'id_comment': id_comment,
                'ups_comment': ups_comment,
                'downs_comment': downs_comment,
                'score_comment': score_comment,
                'permalink_comment': permalink_comment

            }, ignore_index=True)
        except KeyError as e:
            print("With error "+ e)
            print("ERROR on "+post['data']['url'] + " comment: "+comment['data']['id'])

    return df


# params mode = new hot or top
def get_posts(subreddit="magicTCG", mode="hot"):
    request_results = requests.get("https://oauth.reddit.com/r/" + subreddit + "/" + mode + "",
                                   headers=headers)

    df = pd.DataFrame()  # initialize dataframe

    # loop through each post retrieved from GET request
    for post in request_results.json()['data']['children']:
        # append relevant data to dataframe
        df = df.append(get_comment_posts(post['data']['id']))
    return df


results = get_posts()
results.to_csv("results.csv", sep=";", encoding="UTF-8")
