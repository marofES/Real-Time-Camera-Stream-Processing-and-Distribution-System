import asyncio
import aiohttp
import json
from aiohttp import web
from collections import defaultdict
import cv2
from queue import Queue

# Dictionary to store camera queues
camera_queues = defaultdict(Queue)
# Dictionary to store camera capture status
camera_status = defaultdict(bool)


async def handle_ws_message(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Send initial "server is ON" message
    await ws.send_json({"message": "Server is ON"})

    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                cam_id = data.get('cam_id')
                cam_url = data.get('cam_url')
                cam_cap_status = data.get('cam_cap_status')

                if cam_id is not None:
                    if cam_cap_status:
                        if not camera_status[cam_id]:
                            camera_status[cam_id] = True
                            camera_queues[cam_id] = Queue()
                            asyncio.ensure_future(capture_frames(cam_id, cam_url))
                    else:
                        camera_status[cam_id] = False
                        del camera_queues[cam_id]
            except json.JSONDecodeError:
                pass

    return ws


async def capture_frames(cam_id, cam_url):
    cap = cv2.VideoCapture(cam_url)

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if camera_status[cam_id]:
            camera_queues[cam_id].put(frame)

        await asyncio.sleep(0)  # Allow other tasks to run


async def send_frames_to_server(cam_id):
    while True:
        if not camera_queues[cam_id].empty():
            frame = camera_queues[cam_id].get()
            # Send frame to server via API call
            # Example API call code:
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(api_url, data=frame) as response:
            #         response_data = await response.text()
            #         print(response_data)
        await asyncio.sleep(0)  # Allow other tasks to run


async def send_heartbeat_to_clients(ws):
    while True:
        await asyncio.sleep(60)  # Send heartbeat every minute
        await ws.send_json({"heartbeat": "Server is still ON"})


async def main():
    app = web.Application()
    app.add_routes([web.get('/ws', handle_ws_message)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()

    # Keep sending heartbeat messages to clients
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('ws://client-url:port/ws') as ws:
            asyncio.ensure_future(send_heartbeat_to_clients(ws))

    # Start task for sending frames to server for each camera
    for cam_id in camera_queues:
        asyncio.ensure_future(send_frames_to_server(cam_id))

    # Keep the event loop running
    while True:
        await asyncio.sleep(3600)  # Sleep for a long time


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
