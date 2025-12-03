import asyncio
import json
import logging
from fastapi import FastAPI, Request
import httpx
import os
import random

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
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "5.0"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "120.0"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5.0"))

secondary_request_timeouts = {url: REQUEST_TIMEOUT for url in SECONDARIES}


@app.on_event("startup")
async def startup_event():
    global client
    client = httpx.AsyncClient()


@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()


@app.get("/messages")
async def get_messages():
    return {"messages": str(list(messages.values()))}


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


async def send_with_retry(secondary_url: str, msg_item: dict):
    attempt = 0
    url = f"{secondary_url}/replicate"

    while True:
        attempt += 1
        start = asyncio.get_event_loop().time()
        try:
            resp = await client.post(url, json=msg_item, timeout=secondary_request_timeouts[secondary_url])
            resp.raise_for_status()

            rt_new = asyncio.get_event_loop().time() - start
            rt_upd = secondary_request_timeouts[secondary_url]* 0.7 + rt_new * 0.3
            secondary_request_timeouts[secondary_url] = rt_upd
            logger.info(f"updated delay for {secondary_url}: {rt_upd}")
            return resp

        except httpx.HTTPError as e:
            logger.error(f"Error sending to {url}: {e} (msg {msg_item['id']})")

            backoff = max(BASE_BACKOFF * (2 ** (attempt)), secondary_request_timeouts[secondary_url])
            backoff = min(backoff, MAX_BACKOFF)
            jitter = backoff * ((random.random() - 0.5) * 0.2 + 1)
            delay = max(0.01, jitter)

            secondary_request_timeouts[secondary_url] = delay
            logger.info(f"updated delay for {secondary_url}: {delay}")



async def replicate_to_secondaries(msg_item: dict, ask_needed: int):
    initial_ask_needed = ask_needed
    pending_tasks = set()

    for secondary_url in SECONDARIES:
        logger.info(f"Replicating to {secondary_url}")
        task = asyncio.create_task(send_with_retry(secondary_url, msg_item))
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

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}. Message: {msg_item}")



