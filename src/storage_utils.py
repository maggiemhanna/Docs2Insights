from google.cloud import storage

def list_files_in_bucket(bucket_name, prefix, max_files):
    """
    Lists files from a specified Google Cloud Storage bucket folder up to a given maximum number of files.

    Args:
        bucket_name (str): The name of the GCS bucket.
        prefix (str): The folder path in the bucket to list files from.
        max_files (int): The maximum number of files to list.

    Returns:
        list: A list of file URIs (strings) in the format 'gs://<bucket_name>/<file_name>'.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for i, blob in enumerate(blobs):
        if i >= max_files:
            break
        files.append(f"gs://{bucket_name}/{blob.name}")
    
    return files
