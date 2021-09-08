from reddit_api import *
from text_analytics import *
import yaml
import requests

client = authenticate_client()
results = get_posts(subreddit="magicTCG", mode="hot", client_text_analytics=client)
results.to_csv("results.csv", sep=";", encoding="UTF-8")



