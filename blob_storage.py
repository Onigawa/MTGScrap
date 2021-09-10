from azure.storage.blob import BlobServiceClient
import yaml
import os

config = yaml.safe_load(open("config.yaml", 'r'))
account_url = config["blob_storage"]["DefaultEndpointsProtocol"] + "://" + \
              config["blob_storage"]["AccountName"] + \
              ".blob." + config["blob_storage"]["EndpointSuffix"]
account_key = config["blob_storage"]["AccountKey"]
blob_service_client = BlobServiceClient(account_url=account_url,
                                        credential=account_key)


def list_blob_custom(container_name: str):
    """
    List all blob in a blob container

    :param container_name: Name of the container with blob to list
    :return: A list containing the complete path to each blob
    """
    container_client = blob_service_client.get_container_client(container_name)
    # List Blobs
    blob_list = container_client.list_blobs()
    return blob_list


def upload_blob_custom(container_name: str, source_path: str, destination_path: str):
    """
    Upload a file to a blob container
    :param container_name: Name of the destination container
    :param source_path: Path to the local file to be uploaded
    :param destination_path: Path to file to be uploaded to
    """
    # Upload
    container_client = blob_service_client.get_container_client(container_name)
    with open(source_path, "rb") as data:
        blob_client = container_client.get_blob_client(destination_path)
        blob_client.upload_blob(data, overwrite=True)


def download_blob_custom(container_name: str, source_path: str, destination_path: str):
    """
    Download a single blob from path

    :param container_name: Name of the container with desired file
    :param source_path: Path to the file in the container
    :param destination_path: Path to the location in which the file will be downloded

    The destination_path must contain the name of the file. ex: results/data.csv
    """
    container_client = blob_service_client.get_container_client(container_name)
    with open(destination_path, "wb") as my_blob:
        blob_client = container_client.get_blob_client(source_path)
        downloaded_blob = blob_client.download_blob()
        downloaded_blob.readinto(my_blob)


def upload_from_folder(path: str, container_name: str, destination_prefix: str = "", str_filter: str = None):
    """
    Upload all file from a folder
    Str_filter is mostly used to filter .png and .jpeg

    :param path: Path to the folder to be uploaded
    :param container_name: Name of the destination container blob
    :param destination_prefix: Prefix to be added to file in blob
    :param str_filter: Filter to get only files containing the specified string
    """
    container_client = blob_service_client.get_container_client(container_name)
    if str_filter is None:
        files = os.listdir(path)
    else:
        files = os.listdir(path)
        files = [s for s in files if str_filter in s]
    for file in files:
        with open(path + file, "rb") as data:
            blob_client = container_client.get_blob_client(destination_prefix + file)
            blob_client.upload_blob(data, overwrite=True)
