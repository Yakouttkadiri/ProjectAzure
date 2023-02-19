from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import io 
import pandas as pd

#connection_string =  os.environ.get("azure_connection_string")
connection_string =  "DefaultEndpointsProtocol=https;AccountName=storagemst;AccountKey=sDb1Xo09N8w/XxzbgXSq3c+rFmrYM/++w9dd5Bkm+FspB+6mNnRIwDLgF/L6dQMIebyo0/S8uMfP+AStkRwCcg==;EndpointSuffix=core.windows.net"
container_name1 = "containermst"
container_name2 = "output"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client1 = blob_service_client.get_container_client(container_name1)
container_client2 = blob_service_client.get_container_client(container_name2)
blob_list = container_client1.list_blobs(name_starts_with="")

dfs = []

for blob in blob_list:
    stock_name = blob.name.split(".")[0]
    blob_client = container_client1.get_blob_client(blob.name)
    data = blob_client.download_blob().content_as_text()
    df = pd.read_csv(io.StringIO(data), index_col=None, header=0)
    df["Stock Name"] = stock_name
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

output_path = "./output.csv"
combined_df.to_csv(output_path, index=False)

#os.getenv(azure_connection_string, default=None)
with open(output_path, "rb") as data:
    blob_client = container_client2.get_blob_client(output_path)
    blob_client.upload_blob(data)
