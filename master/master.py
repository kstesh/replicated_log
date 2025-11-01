import asyncio
import json
import logging
from fastapi import FastAPI, Request
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Master")

app = FastAPI()
messages = []
message_counter = 0


with open("config.json") as f:
    CONFIG = json.load(f)
SECONDARIES = CONFIG["secondaries"]

@app.get("/messages")
async def get_messages():
    return {"messages": str(messages)}

@app.post("/append")
async def append_message(request: Request):
    global message_counter

    message_counter += 1
    msg = message_counter

    messages.append(msg)
    logger.info(f"Appended message ID locally: {msg}")

    await replicate_to_secondaries(msg)

    return {"status": "ok", "message": msg}



async def replicate_to_secondaries(message: int):
    async with httpx.AsyncClient() as client:
        tasks = []
        for secondary_url in SECONDARIES:
            url = f"{secondary_url}/replicate"
            logger.info(f"Replicating to {url}")
            tasks.append(client.post(url, json={"message": message}))
        responses = await asyncio.gather(*tasks)
        for resp in responses:
            logger.info(f"ACK from secondary: {resp.status_code}")