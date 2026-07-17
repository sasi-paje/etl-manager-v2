@echo off
cd /d C:\ETL\etl-manager-v2
set PYTHONUNBUFFERED=1
for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
  if not "%%A"=="" if not "%%A:~0,1"=="#" set "%%A=%%B"
)
C:\ETL\etl-manager-v2\.venv\Scripts\python.exe -m etl_manager.daemon
