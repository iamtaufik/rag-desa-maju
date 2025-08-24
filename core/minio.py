from fastapi import UploadFile
from minio import Minio, S3Error
from settings import (
    BUCKET_NAME,
    MINIO_HOST,
    MINIO_PORT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    LOCAL_STORAGE_PATH
)
from datetime import datetime
from io import BytesIO
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FileProcessor:
    def __init__(self) -> None:
        self._client = Minio(
            f"{MINIO_HOST}:{MINIO_PORT}",
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Change to True if using HTTPS
        )

        if not self._client.bucket_exists(BUCKET_NAME):
            self._client.make_bucket(BUCKET_NAME)

    async def upload_to_minio(self, file: UploadFile):
        # Placeholder for MinIO upload logic
        print(f"Uploading {file.filename} to bucket {BUCKET_NAME}...")
        try:
            contents = await file.read()
            file_size = len(contents)
            file_name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}"
            file_path = f"{BUCKET_NAME}/{file_name}"
            file_content_stream = BytesIO(contents)

            uploaded_file = self._client.put_object(
                BUCKET_NAME,
                file_name,
                file_content_stream,
                file_size,
                content_type=file.content_type
            )
            print(f"File {file_name} uploaded successfully to {file_path}.")
            return {
                "file_name": file_name,
                "file_path": file_path,
                "content_type": file.content_type,
                "size": file_size,
                "upload_time": datetime.now().isoformat()
            }
        except S3Error as e:
            raise Exception(f"MinIO error: {e}")

        except Exception as e:
            raise Exception(f"Failed to upload file: {e}")
        
    def download_from_minio_to_local(self, file_name: str):
        try:
            logging.info(f"Downloading {file_name} from bucket {BUCKET_NAME} to local storage...")
            local_path = f"{LOCAL_STORAGE_PATH}/documents/{file_name}"
            self._client.fget_object(bucket_name=BUCKET_NAME, object_name=file_name, file_path=local_path)
            logging.info(f"File {file_name} downloaded successfully to {local_path}.")
            return {
                'file_name': file_name,
                'local_path': local_path,
            }
        except S3Error as e:
            raise Exception(f"MinIO error: {e}")
        except Exception as e:
            raise Exception(f"Failed to download file: {e}")
        
    def delete_from_minio(self, file_name: str) -> None:
        try:
            self._client.remove_object(BUCKET_NAME, file_name)
            print(f"File {file_name} deleted successfully from bucket {BUCKET_NAME}.")
        except S3Error as e:
            raise Exception(f"MinIO error: {e}")
        except Exception as e:
            raise Exception(f"Failed to delete file: {e}")