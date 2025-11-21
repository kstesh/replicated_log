import asyncio
import logging
from fastapi import FastAPI, Request
from sortedcontainers import SortedDict
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Secondary")

REPLICATION_DELAY = int(os.getenv("REPLICATION_DELAY", "5"))

app = FastAPI()
messages = SortedDict()

@app.get("/messages")
async def get_messages():
    return {"messages": str(messages.values()[:])}

@app.post("/replicate")
async def replicate_message(request: Request):
    msg_item = await request.json()

    await asyncio.sleep(REPLICATION_DELAY)

    logger.info(f"Received replicated message: {msg_item}")

    if messages.__contains__(msg_item["id"]):
        logger.info(f"Received duplicated message: {msg_item['id']}")
        return {"status": "ACK", "message": f"duplicate message # {msg_item['id']}"}

    messages.setdefault(msg_item["id"], msg_item["message"])
    return {"status": "ACK", "message": messages[msg_item["id"]]}