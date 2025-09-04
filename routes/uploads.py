from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from core.minio import FileProcessor
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

router = APIRouter(tags=["Upload File"])

@router.post("/upload")
async def upload_file(file: UploadFile = File()):
    accepted_extensions = ['.pdf', '.docx', '.txt']
    if not any(file.filename.endswith(ext) for ext in accepted_extensions):
        return JSONResponse(content={"message": "Unsupported file type."}, status_code=400)
    try:
        
        file_processor = FileProcessor()
        result = await file_processor.upload_to_minio(file)
        try:
            from main import rabbitmq_producer

            if not rabbitmq_producer:
                return JSONResponse(content={"message": "RabbitMQ producer is not available."}, status_code=500)
            else:
                rabbitmq_result = rabbitmq_producer.publish(
                    message={
                        "file_name": result["file_name"],
                        "file_path": result["file_path"],
                        "content_type": result["content_type"],
                        "size": result["size"],
                        "upload_time": result["upload_time"],
                        "action": "file_uploaded"
                    }
                )
        except Exception as e:
            if result:
                logging.error(f"Failed to publish message to RabbitMQ: {str(e)}")
                file_processor.delete_from_minio(result["file_name"])
            raise Exception(f"Failed to publish message to RabbitMQ: {str(e)}")

        return JSONResponse(content={"filename": file.filename, "message": result}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"message": f"Failed to upload file: {str(e)}"}, status_code=500)