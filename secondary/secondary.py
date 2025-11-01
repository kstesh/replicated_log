import asyncio
import logging
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Secondary")

app = FastAPI()
messages = []

@app.get("/messages")
async def get_messages():
    return {"messages": str(messages)}

@app.post("/replicate")
async def replicate_message(request: Request):
    data = await request.json()
    msg = data.get("message")
    logger.info(f"Received replicated message: {msg}")

    await asyncio.sleep(2)
    messages.append(msg)
    return {"status": "ACK", "message": msg}