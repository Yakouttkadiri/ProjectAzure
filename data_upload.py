import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os



# Connection string for your Azure Storage account
connect_str = os.environ.get('azure_connection_string')

# Create the BlobServiceClient object which will be used to create a container client

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Create a container client object for your container in Azure Blob Storage
container_name = "containermst"
container_client = blob_service_client.get_container_client(container_name)


# Create a function to upload files to your container in Azure Blob Storage
def upload_to_azure_storage(file_path, file_name):
    blob_client = container_client.get_blob_client(blob=file_name)
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data)

csv_file_path = "./stocks_data/"
csv_file_name = ["AMAZON.csv", "APPLE.csv", "FACEBOOK.csv", "GOOGLE.csv", "MICROSOFT.csv", "TESLA.csv", "ZOOM.csv"]
for stock in csv_file_name:
    blob_client = container_client.get_blob_client(stock)
    print(csv_file_path+stock)
    with open(csv_file_path+stock, "rb") as data:
        blob_client.upload_blob(data)
