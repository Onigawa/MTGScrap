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



def list_blob_custom(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    # List Blobs
    blob_list = container_client.list_blobs()
    return blob_list


def upload_blob_custom(container_name, source_path, destination_path):
    # Upload
    container_client = blob_service_client.get_container_client(container_name)
    with open(source_path, "rb") as data:
        blob_client = container_client.get_blob_client(destination_path)
        blob_client.upload_blob(data, overwrite=True)


def download_blob_custom(container_name, source_path, destination_path):
    # Download
    container_client = blob_service_client.get_container_client(container_name)
    with open(destination_path, "wb") as my_blob:
        blob_client = container_client.get_blob_client(source_path)
        downloaded_blob = blob_client.download_blob()
        downloaded_blob.readinto(my_blob)


def upload_from_folder(path, container_name, prefix: str = "", suffix: str = None):
    container_client = blob_service_client.get_container_client(container_name)
    if suffix is None:
        files = os.listdir(path)
    else:
        files = os.listdir(path)
        files = [s for s in files if suffix in s]
    for file in files:
        with open(path + file, "rb") as data:
            blob_client = container_client.get_blob_client(prefix + file)
            blob_client.upload_blob(data,overwrite=True)
