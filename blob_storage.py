from azure.storage.blob import BlobServiceClient
import yaml
import os

config = yaml.safe_load(open("config.yaml", 'r'))
account_url = config["blob_storage"]["DefaultEndpointsProtocol"] + "://" + \
              config["blob_storage"]["AccountName"] + \
              ".blob." + config["blob_storage"]["EndpointSuffix"]
account_key = config["blob_storage"]["AccountKey"]
connexion_string = config["blob_storage"]["DefaultConnexionString"]
blob_service_client = BlobServiceClient(account_url=account_url,
                                        credential=account_key)
container_client_azure = blob_service_client.get_container_client(config["blob_storage"]["ContainerName"])


def list_blob_custom(container_client):
    # List Blobs
    blob_list = container_client.list_blobs()
    return blob_list


def upload_blob_custom(container_client, source_path, destination_path):
    # Upload
    with open(source_path, "rb") as data:
        blob_client = container_client.get_blob_client(destination_path)
        blob_client.upload_blob(data, overwrite=False)


def download_blob_custom(container_client, source_path, destination_path):
    # Download
    with open(destination_path, "wb") as my_blob:
        blob_client = container_client.get_blob_client(source_path)
        downloaded_blob = blob_client.download_blob()
        downloaded_blob.readinto(my_blob)


def upload_from_folder(path, container_client, prefix: str = ""):
    for file in os.listdir(path):
        with open(path + file, "rb") as data:
            blob_client = container_client.get_blob_client(prefix + file)
            blob_client.upload_blob(data)
