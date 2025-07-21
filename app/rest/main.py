from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, Query, HTTPException
from datetime import date, timedelta, datetime
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

@app.get("/api/ranking-movimentacao")
def get_ranking_movimentacao():
    """Retorna o ranking de movimentação de veículos"""
    try:
        ranking_data = RepositoryScreeningCvm.ranking_movimentacao_veiculos()
        return {"data": ranking_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar ranking de movimentação: {str(e)}")

@app.get("/api/alocacao-por-ativo")
def get_alocacao_por_ativo():
    """Retorna a distribuição de alocação por ativo de todos os veículos"""
    try:
        alocacao_data = RepositoryScreeningCvm.alocacao_por_ativo()
        return alocacao_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar dados de alocação: {str(e)}")

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
    """
    Retorna a lâmina do fundo com série de cotas e composição da carteira.
    """
    try:
        from repository.repository_screening_cvm import RepositoryScreeningCvm
        data = RepositoryScreeningCvm.lamina_fundo(cnpj_fundo_classe, tp_fundo_classe)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/contagem-fundos-unicos-acoes")
def get_contagem_fundos_unicos_acoes():
    """
    Retorna a contagem de fundos únicos por data que investem em ações.
    """
    try:
        from repository.repository_screening_cvm import RepositoryScreeningCvm
        data = RepositoryScreeningCvm.contagem_fundos_unicos_por_data()
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/ranking-patrimonio-acoes-periodo")
def get_ranking_patrimonio_acoes_periodo(
    data_inicio: str = Query(..., description="Data de início (YYYY-MM-DD)"),
    data_fim: str = Query(..., description="Data de fim (YYYY-MM-DD)")
):
    """
    Retorna o ranking de patrimônio sob gestão em ações para um período específico.
    """
    try:
        from datetime import datetime
        from repository.repository_screening_cvm import RepositoryScreeningCvm
        
        data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d").date()
        data_fim_dt = datetime.strptime(data_fim, "%Y-%m-%d").date()
        
        data = RepositoryScreeningCvm.ranking_patrimonio_acoes_por_periodo(data_inicio_dt, data_fim_dt)
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
