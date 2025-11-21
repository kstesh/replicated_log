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

    await replicate_to_secondaries(msg_item, w)

    return {"status": "ok", "message": incoming_message}



async def replicate_to_secondaries(msg_item: dict, w: int):
    global client

    pending_tasks = set()
    for secondary_url in SECONDARIES:
        url = f"{secondary_url}/replicate"
        logger.info(f"Replicating to {url}")
        task = asyncio.create_task(client.post(url, json=msg_item, timeout=30.0))
        pending_tasks.add(task)

    if w == 1:
        logger.info(f"Master only ACK received. Message: {msg_item}")
        return

    ack_needed = w - 1
    ack_count = 0

    while ack_count < ack_needed:
        # 3. Wait for the *next* single task to complete
        done_tasks, pending_tasks = await asyncio.wait(
            pending_tasks,
            return_when=asyncio.FIRST_COMPLETED
        )


        for task in done_tasks:
            try:
                response = await task
                logger.info(f"ACK from secondary: {response.status_code}. Message: {response.message}")
                ack_count+=1
                if ack_count == ack_needed:
                    logger.info(f"Required {w} ACKs received.")
                    return
            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error during replication: {e}")

            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")



