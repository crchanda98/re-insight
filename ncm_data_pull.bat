@echo off

set LOG_DIR=D:\work\git_manage\logfiles
set GIT_DIR=D:\work\git_manage

cd /d %GIT_DIR%\re-insight

call C:\Users\arijit\miniconda3\Scripts\activate.bat weather

python ncm_data_pull.py >> %LOG_DIR%\log.ncm_data_pull.txt 2>&1
python openmeteo_point_data.py >> %LOG_DIR%\log.openmeteo_point_data.txt 2>&1
python ncm_ad_data_pull.py >> %LOG_DIR%\log.ncm_ad_data_pull.txt 2>&1
