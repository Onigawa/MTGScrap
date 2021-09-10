from reddit_api import *
from text_analytics import *
from blob_storage import *


client = authenticate_client()
results = get_posts(subreddit="magicTCG", mode="hot", client_text_analytics=None)
results.to_csv("results.csv", sep=";", encoding="UTF-8")

split(results, "results/")
upload_from_folder(path="results/", container_name="textdocuments", str_filter=".txt")
upload_from_folder(path="results/", container_name="imagedocuments", str_filter=".png")
upload_from_folder(path="results/", container_name="imagedocuments", str_filter=".jpg")
