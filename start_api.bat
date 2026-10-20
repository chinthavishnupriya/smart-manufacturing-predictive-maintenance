@echo off
cd /d "%~dp0"
call .venv\Scripts\activate
uvicorn api.main:app --reload
pause
