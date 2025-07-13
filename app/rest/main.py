from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect
from datetime import date, timedelta
from repository.repository_screening_cvm import RepositoryScreeningCvm

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
    "http://localhost:80",
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

@app.get("/api/v1/ranking-movimentacao")
def get_ranking_movimentacao_ranges():
    try:
        ranking_dia = RepositoryScreeningCvm.ranking_movimentacao_veiculos()
                
        return {
            "dia": ranking_dia,
            "7_dias": [],
            "31_dias": []
        }
    except Exception as e:
        print(f"Erro na busca: {str(e)}")
        return {"error": str(e)}

@app.get("/api/v1/ranking-gestores-por-patrimonio-sob-gestao")
def get_ranking_gestores_por_patrimonio_sob_gestao():
    """Retorna o ranking dos gestores por patrimônio sob gestão"""
    try:
        ranking = RepositoryScreeningCvm.pega_rank_gestores_por_patrimonio_sob_gestao()
        return {"ranking": ranking}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
