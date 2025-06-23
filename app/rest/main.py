from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect

from interface_websocket import WebsocketConnectionManager
from settings import FIREBASE_CREDENTIAL_FILE
from firebase_admin import credentials, initialize_app
from services.firebase import get_user_token
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

cred = credentials.Certificate(FIREBASE_CREDENTIAL_FILE)
initialize_app(cred)
manager = WebsocketConnectionManager()


@app.get("/api/v1/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/protected")
def protected_route(user: dict = Depends(get_user_token)):
    return {"user": user}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, user: dict = Depends(get_user_token)):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await manager.send_personal_message(f"You wrote: {data}", websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
