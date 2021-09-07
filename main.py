from reddit_api import *
from text_analytics import *
import yaml
import requests

config = yaml.safe_load(open("config.yaml", 'r'))

REDDIT_CLIENT_ID = config["reddit_api"]["client_id"]
REDDIT_SECRET_KEY = config["reddit_api"]["secret_key"]
REDDIT_PASSWORD = config["reddit_api"]["password"]
REDDIT_USERNAME = config["reddit_api"]["username"]

auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET_KEY)
data = {'grant_type': 'password',
        'username': REDDIT_USERNAME,
        'password': REDDIT_PASSWORD}
headers = {'User-Agent': 'MTGScrap/0.0.1'}
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)
TOKEN = res.json()['access_token']
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
requests.get('https://oauth.reddit.com/api/v1/me', headers=headers)

client = authenticate_client()
results = get_posts(headers=headers, subreddit="magicTCG", mode="hot", client_text_analytics=client)
results.to_csv("results.csv", sep=";", encoding="UTF-8")



