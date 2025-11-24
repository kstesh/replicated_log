import asyncio
import json
import logging
from fastapi import FastAPI, Request
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Master")

app = FastAPI()
client: httpx.AsyncClient = None

messages = {}
message_counter = 0

lock = asyncio.Lock()

with open("config.json") as f:
    CONFIG = json.load(f)
SECONDARIES = CONFIG["secondaries"]


@app.on_event("startup")
async def startup_event():
    global client
    client = httpx.AsyncClient()

@app.on_event("shutdown")
async def shutdown_event():
    global client
    await client.aclose()

@app.get("/messages")
async def get_messages():
    return {"messages": str(messages.values())}

@app.post("/append")
async def append_message(request: Request):
    global message_counter

    data = await request.json()

    w = int(data.get("w"))
    if not (1 <= w <= len(SECONDARIES) + 1):
        return {"error": f"write concern in range [1, {len(SECONDARIES)+1}] is expected."}

    incoming_message = data.get("message")
    if not incoming_message:
        return {"error": "message field required"}

    async with lock:
        message_counter += 1
        messages[message_counter] = incoming_message

        msg_item = {
            "id": message_counter,
            "message": incoming_message
        }

        logger.info(f"Appended message locally: {msg_item}")

    await replicate_to_secondaries(msg_item, w - 1)

    return {"status": "ok", "message": incoming_message}



async def replicate_to_secondaries(msg_item: dict, ask_needed: int):
    global client
    initial_ask_needed = ask_needed
    pending_tasks = set()

    for secondary_url in SECONDARIES:
        url = f"{secondary_url}/replicate"
        logger.info(f"Replicating to {url}")
        task = asyncio.create_task(client.post(url, json=msg_item, timeout=30.0))
        pending_tasks.add(task)

    if ask_needed == 0:
        logger.info(f"Master only ACK received. Message: {msg_item}")
        return

    for earliest_task in asyncio.as_completed(pending_tasks):

        try:
            response = await earliest_task
            logger.info(f"ACK from secondary: {response.status_code}. Message: {msg_item}")
            ask_needed -= 1
            if ask_needed == 0:
                logger.info(f"Required {initial_ask_needed} ACKs received.")
                return
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error during replication: {e}")

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")



