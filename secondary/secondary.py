import asyncio
import logging
from fastapi import FastAPI, Request
from sortedcontainers import SortedDict
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Secondary")
lock = asyncio.Lock()

REPLICATION_DELAY = int(os.getenv("REPLICATION_DELAY", "5"))

app = FastAPI()
messages = SortedDict()

@app.get("/messages")
async def get_messages():
    keys = messages.keys()
    n = len(keys)

    if n == 0 or n == keys[-1]:
        return {"messages": str(messages.values()[:])}

    low = 0
    high = n - 1
    slice_index = None

    while low <= high:
        mid = (low + high) // 2
        if keys[mid] != mid + 1:
            slice_index = mid
            high = mid - 1
        else:
            low = mid + 1
    return {"messages": str(messages.values()[:slice_index])}

@app.post("/replicate")
async def replicate_message(request: Request):
    msg_item = await request.json()

    await asyncio.sleep(REPLICATION_DELAY)

    async with lock:
        if msg_item["id"] in messages:
            logger.info(f"Received duplicated message: {msg_item['id']}")
        else:
            messages.setdefault(msg_item["id"], msg_item["message"])
            logger.info(f"Received replicated message: {msg_item}")
    return {"status": "ACK"}