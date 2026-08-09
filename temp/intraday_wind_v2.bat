@echo off

set LOG_DIR=D:\work\git_manage\logfiles
set GIT_DIR=D:\work\git_manage

cd /d %GIT_DIR%\re-insight

call C:\Users\arijit\miniconda3\Scripts\activate.bat weather

python intraday_wind_v2.py >> %LOG_DIR%\log.intraday_wind_v2.txt 2>&1

python intraday_wind_v3.py >> %LOG_DIR%\log.intraday_wind_v3.txt 2>&1

python intraday_wind_ts_model.py >> %LOG_DIR%\log.intraday_wind_ts_model.txt 2>&1

python intraday_solar_rf.py >> %LOG_DIR%\log.intraday_solar_rf.txt 2>&1

@REM python intraday_solar_ts.py >> %LOG_DIR%\log.intraday_solar_ts.txt 2>&1

python fct_dispatch_solar.py >> %LOG_DIR%\log.fct_dispatch_solar.txt 2>&1

python fct_dispatch_pc.py >> %LOG_DIR%\log.fct_dispatch_pc.txt 2>&1

python aggregator.py >> %LOG_DIR%\log.aggregator.txt 2>&1