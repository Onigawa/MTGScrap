import yaml
import requests
import pandas as pd

config = yaml.safe_load(open("config.yaml", 'r'))

CLIENT_ID = config.get("client_id")
SECRET_KEY = config.get("secret_key")
auth = requests.auth.HTTPBasicAuth(CLIENT_ID,SECRET_KEY)

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

res = requests.get("https://oauth.reddit.com/r/magicTCG/new",
                   headers=headers)

df = pd.DataFrame()  # initialize dataframe

# loop through each post retrieved from GET request
for post in res.json()['data']['children']:
    # append relevant data to dataframe
    df = df.append({
        'subreddit': post['data']['subreddit'],
        'title': post['data']['title'],
        'selftext': post['data']['selftext'],
        'url': post['data']['url'],
        'upvote_ratio': post['data']['upvote_ratio'],
        'ups': post['data']['ups'],
        'downs': post['data']['downs'],
        'score': post['data']['score']
    }, ignore_index=True)

df.to_csv("results.csv",sep=";")