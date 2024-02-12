from azure.storage.blob import BlobServiceClient

def set_connection_blob():
        
    try:
        account_name = 'scgpkgdldevhot'
        account_key = 'p07GLBmSnUJpePQluB/p70ibuSUX6ny4w7awxOQOmL8+ospEQ1Xc59jMBu7e5113D1s8DCSo8kzis5z9gHC2nA=='
        connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)   
        return  blob_service_client

    except Exception as e:
        print(e)
         