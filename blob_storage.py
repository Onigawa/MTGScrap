from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions, \
    BlobClient
import yaml

config = yaml.safe_load(open("config.yaml", 'r'))
account_url = config["blob_storage"]["DefaultEndpointsProtocol"] + "://" + \
              config["blob_storage"]["AccountName"] + \
              ".blob." + config["blob_storage"]["EndpointSuffix"]
account_key = config["blob_storage"]["AccountKey"]
connexion_string = config["blob_storage"]["DefaultConnexionString"]
blob_service_client = BlobServiceClient(account_url=account_url,
                                        credential=account_key)


container_client = blob_service_client.get_container_client('textdocuments')

# List Blobs
blob_list = container_client.list_blobs()
for blob in blob_list:
    print(blob.name + '\n')


# Upload
with open("./test.txt", "rb") as data:
    blob_client = container_client.get_blob_client('test.txt')
    blob_client.upload_blob(data)

# Download
with open("./BlockDestination.txt", "wb") as my_blob:
    blob_client = container_client.get_blob_client('test.txt')
    downloaded_blob = blob_client.download_blob()
    downloaded_blob.readinto(my_blob)
