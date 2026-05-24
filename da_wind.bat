@echo off

set LOG_DIR=D:\work\git_manage\logfiles
set GIT_DIR=D:\work\git_manage

cd /d %GIT_DIR%\re-insight

call C:\Users\arijit\miniconda3\Scripts\activate.bat weather

python da_wind.py >> %LOG_DIR%\log.da_wind.txt 2>&1