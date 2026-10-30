import os
import json
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from kafka.errors import KafkaError
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Initialize Rate Limiter based on client IP
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Clickstream Ingestion API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 1. Strict CORS Configuration
origins = [
    "http://localhost",
    "http://127.0.0.1",
    "http://localhost:80",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "clickstream-raw"

# Initialize Kafka Producer with JSON serialization
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        request_timeout_ms=5000
    )
    print(f"Connected to Kafka broker at {KAFKA_BROKER}")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    producer = None

# Strict schema validation to secure against malformed or malicious payloads
class EventPayload(BaseModel):
    event_id: str
    event_type: str = Field(..., max_length=50)
    url: str = Field(..., max_length=2000)
    referrer: Optional[str] = Field(None, max_length=2000)
    timestamp: str
    screen_width: Optional[int] = None
    screen_height: Optional[int] = None
    element_tag: Optional[str] = Field(None, max_length=50)
    element_id: Optional[str] = Field(None, max_length=100)
    element_classes: Optional[str] = Field(None, max_length=255)
    element_text: Optional[str] = Field(None, max_length=200)
    target_href: Optional[str] = Field(None, max_length=2000)
    user_id: Optional[str] = Field(None, max_length=100)

@app.post("/api/collect")
@limiter.limit("100/60minute")  # Rate limit: Max 100 requests per 60 minutes per IP
async def collect_event(request: Request):
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer is offline")

    # Payload size protection (Max 10KB check via Content-Length header)
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 10240:
        raise HTTPException(status_code=413, detail="Payload too large")

    try:
        body = await request.json()
        event = EventPayload(**body)
    except Exception as err:
        raise HTTPException(status_code=400, detail=f"Invalid payload format: {str(err)}")

    # Enrich server-side data securely (Never trust client timestamps or IPs blindly)
    client_ip = request.headers.get("x-forwarded-for") or request.client.host
    event_dict = event.model_dump()
    event_dict["server_received_at"] = datetime.utcnow().isoformat()
    event_dict["client_ip"] = client_ip

    try:
        # Push message asynchronously to Kafka topic with user_id key
        message_key = (event.user_id or "anonymous").encode('utf-8')
        future = producer.send(KAFKA_TOPIC, key=message_key, value=event_dict)
        future.get(timeout=2)
    except KafkaError as ke:
        print(f"Kafka error: {ke}")
        raise HTTPException(status_code=500, detail="Internal storage error")

    # Respond with 204 No Content for high performance beacon tracking
    return Response(status_code=204)

@app.get("/health")
def health_check():
    return {"status": "healthy", "kafka_connected": producer is not None}