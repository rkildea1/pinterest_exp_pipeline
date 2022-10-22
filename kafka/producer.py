from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()



class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    
    streaming_upload_record = dict(item) 

    pinterest_upload_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Streaming Upload Producer",
    value_serializer=lambda streaming_upload_record: dumps(streaming_upload_record).encode("ascii")
    ) 
    pinterest_upload_producer.send(topic="pinterest_streaming_uploads", value=streaming_upload_record)

    # pinterest_upload_producer.send(topic="pinterestuploads", value=streaming_upload_record)

    return streaming_upload_record


if __name__ == '__main__':
    uvicorn.run("producer:app", host="localhost", port=8000)




