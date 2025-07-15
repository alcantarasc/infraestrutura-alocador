@echo off
setlocal enabledelayedexpansion

echo ========================================
echo Iniciando aplicacao Alocador
echo ========================================

REM Verifica se estamos no diretório correto
echo Verificando estrutura de diretorios...
if not exist "rest\requirements.txt" (
    echo ERRO: Arquivo rest\requirements.txt nao encontrado
    echo Certifique-se de executar este script na pasta app
    echo Diretorio atual: %CD%
    pause
    exit /b 1
)

if not exist "front-end\package.json" (
    echo ERRO: Arquivo front-end\package.json nao encontrado
    echo Certifique-se de executar este script na pasta app
    echo Diretorio atual: %CD%
    pause
    exit /b 1
)

echo Estrutura de diretorios OK!

REM Verifica se Python está instalado
echo Verificando Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python nao encontrado. Instale o Python primeiro.
    pause
    exit /b 1
)
echo Python encontrado!

REM Verifica se Node.js está instalado
echo Verificando Node.js...
node --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Node.js nao encontrado. Instale o Node.js primeiro.
    pause
    exit /b 1
)
echo Node.js encontrado!

REM Cria/ativa a venv
echo.
echo Configurando ambiente Python...
if exist "venv\Scripts\activate.bat" (
    echo Virtual environment encontrado, ativando...
    call venv\Scripts\activate.bat
) else (
    echo Criando virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo ERRO: Falha ao criar virtual environment
        pause
        exit /b 1
    )
    echo Virtual environment criado com sucesso!
    call venv\Scripts\activate.bat
    echo Instalando dependencias do back-end...
    pip install -r rest\requirements.txt
    if errorlevel 1 (
        echo ERRO: Falha ao instalar dependencias do back-end
        pause
        exit /b 1
    )
    echo Dependencias do back-end instaladas!
)

REM Instala dependências do front-end se necessário
echo.
echo Configurando ambiente Node.js...
if not exist "front-end\node_modules" (
    echo Instalando dependencias do front-end...
    cd front-end
    npm install
    if errorlevel 1 (
        echo ERRO: Falha ao instalar dependencias do front-end
        pause
        exit /b 1
    )
    cd ..
    echo Dependencias do front-end instaladas!
) else (
    echo Dependencias do front-end ja instaladas!
)

echo.
echo ========================================
echo Iniciando servicos...
echo ========================================

REM Inicia o back-end em background
echo Iniciando API (FastAPI) na porta 8000...
start "API Backend" cmd /k "cd /d %CD% && call venv\Scripts\activate.bat && cd rest && python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload"

REM Aguarda um pouco para o back-end inicializar
echo Aguardando inicializacao do back-end...
timeout /t 5 /nobreak > nul

REM Inicia o front-end em background
echo Iniciando Frontend (React) na porta 3000...
start "Frontend React" cmd /k "cd /d %CD%\front-end && npm start"

echo.
echo ========================================
echo Aplicacao iniciada com sucesso!
echo ========================================
echo.
echo URLs de acesso:
echo - Frontend: http://localhost:3000
echo - API Backend: http://localhost:8000
echo - API Docs: http://localhost:8000/docs
echo.
echo Servicos iniciados em janelas separadas do CMD
echo Para parar os servicos, feche as janelas do CMD
echo.
echo Pressione qualquer tecla para sair...
pause > nul 