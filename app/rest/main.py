from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, Query
from datetime import date, timedelta
from repository.repository_screening_cvm import RepositoryScreeningCvm
from typing import Optional

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
def get_ranking_movimentacao_ranges(
    page: Optional[int] = Query(0, ge=0, description="Número da página (começando em 0)"),
    page_size: Optional[int] = Query(25, ge=1, le=100, description="Tamanho da página"),
    periodo: Optional[str] = Query("dia", description="Período: dia, 7_dias, 31_dias")
):
    try:
        # Calcula offset baseado na página
        offset = page * page_size
        
        ranking_data = RepositoryScreeningCvm.ranking_movimentacao_veiculos_paginado(
            offset=offset, 
            limit=page_size,
            periodo=periodo
        )
        
        return {
            "data": ranking_data["data"],
            "total": ranking_data["total"],
            "page": page,
            "page_size": page_size,
            "total_pages": (ranking_data["total"] + page_size - 1) // page_size
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

@app.get("/api/v1/lamina-fundo")
def get_lamina_fundo(
    cnpj_fundo_classe: str = Query(..., description="CNPJ do fundo"),
    tp_fundo_classe: str = Query(..., description="Tipo do fundo")
):
    """Retorna a lâmina do fundo com série de cotas e composição da carteira"""
    try:
        lamina = RepositoryScreeningCvm.lamina_fundo(
            cnpj_fundo_classe=cnpj_fundo_classe,
            tp_fundo_classe=tp_fundo_classe
        )
        return lamina
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
