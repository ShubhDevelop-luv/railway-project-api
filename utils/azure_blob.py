# Handles Azure Blob Storage operations
import os
import logging
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv



# Load environment variables
load_dotenv()

class BlobStorage:
    def __init__(self):
        """
        Initializes connection to Azure Blob Storage.
        """
        try:
            self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

            if not self.connection_string:
                raise ValueError("Missing Azure Storage connection string in environment variables.")

            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

        except Exception as e:
            logging.error(f"Failed to initialize BlobStorage: {str(e)}")
            raise

    def get_container_client(self, container_name: str):
        """
        Retrieves the specified container client, creating it if it does not exist.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)

            if not container_client.exists():
                logging.warning(f"Container '{container_name}' does not exist. Creating...")
                container_client.create_container()

            return container_client

        except Exception as e:
            logging.error(f"Failed to get container '{container_name}': {str(e)}")
            raise

    def upload_file(self, file_name: str, file_data, container_name: str, content_type="application/octet-stream") -> str:
        """
        Uploads a file to a specific Azure Blob Storage container.
        
        :param file_name: Name of the blob (unique filename)
        :param file_data: File stream or binary data
        :param container_name: Name of the Azure Blob container
        :param content_type: MIME type of the file
        :return: URL of the uploaded file
        """
        try:
            container_client = self.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob=file_name)

            blob_client.upload_blob(file_data, overwrite=True, content_settings=ContentSettings(content_type=content_type))
            logging.info(f"File '{file_name}' uploaded successfully to '{container_name}'.")

            return blob_client.url

        except Exception as e:
            logging.error(f"Upload failed for '{file_name}' in '{container_name}': {str(e)}")
            raise

    def download_file(self, file_name: str, container_name: str) -> bytes:
        """
        Downloads a file from Azure Blob Storage.
        
        :param file_name: Name of the blob to download
        :param container_name: Name of the Azure Blob container
        :return: File content in bytes
        """
        try:
            container_client = self.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob=file_name)

            if not blob_client.exists():
                raise FileNotFoundError(f"Blob '{file_name}' not found in container '{container_name}'.")

            file_data = blob_client.download_blob().readall()
            logging.info(f"File '{file_name}' downloaded successfully.")
            return file_data

        except FileNotFoundError as e:
            logging.error(str(e))
            raise
        except Exception as e:
            logging.error(f"Download failed for '{file_name}' in '{container_name}': {str(e)}")
            raise

    def delete_file(self, file_name: str, container_name: str) -> bool:
        """
        Deletes a file from Azure Blob Storage.

        :param file_name: Name of the blob to delete
        :param container_name: Name of the Azure Blob container
        :return: True if deletion was successful, False if file did not exist
        """
        try:
            container_client = self.get_container_client(container_name)
            blob_client = container_client.get_blob_client(blob=file_name)

            if not blob_client.exists():
                logging.warning(f"Blob '{file_name}' does not exist in '{container_name}'.")
                return False

            blob_client.delete_blob()
            logging.info(f"File '{file_name}' deleted successfully from '{container_name}'.")
            return True

        except Exception as e:
            logging.error(f"Deletion failed for '{file_name}' in '{container_name}': {str(e)}")
            raise