import yaml
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
import pandas as pd

config = yaml.safe_load(open("config.yaml", 'r'))

key = config["text_analytics"]["secret_key"]
endpoint = config["text_analytics"]["endpoint"]


def authenticate_client():
    ta_credential = AzureKeyCredential(key)
    text_analytics_client = TextAnalyticsClient(
        endpoint=endpoint,
        credential=ta_credential)
    return text_analytics_client


def sentiment_analysis(client, documents=None):
    if documents is None:
        documents = "I had the best day of my life. I wish you were there with me."
    if len(documents) < 3:
        df = pd.DataFrame({
            "document_sentiment": "Empty",
            "document_confidence_positive": [0.0],
            "document_confidence_neutral": [0.0],
            "document_confidence_negative": [0.0],
            "text_analysed": "",
            "sentence_number": [0],
            "sentence_sentiment": "Empty",
            "sentence_confidence_positive": [0.0],
            "sentence_confidence_neutral": [0.0],
            "sentence_confidence_negative": [0.0],
        })
        return df

    df = pd.DataFrame()
    response = client.analyze_sentiment(documents=[documents])[0]
    confidence_positive = response.confidence_scores.positive
    confidence_neutral = response.confidence_scores.neutral
    confidence_negative = response.confidence_scores.negative
    document_sentiment = response.sentiment

    for idx, sentence in enumerate(response.sentences):
        df = df.append({
            "document_sentiment": document_sentiment,
            "document_confidence_positive": confidence_positive,
            "document_confidence_neutral": confidence_neutral,
            "document_confidence_negative": confidence_negative,
            "text_analysed": sentence.text,
            "sentence_number": idx,
            "sentence_sentiment": sentence.sentiment,
            "sentence_confidence_positive": sentence.confidence_scores.positive,
            "sentence_confidence_neutral": sentence.confidence_scores.neutral,
            "sentence_confidence_negative": sentence.confidence_scores.negative,
        }, ignore_index=True)
    return df
