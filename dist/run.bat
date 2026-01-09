@echo off
set HOSTNAME=localhost
set PORT=7165
set WINDOW_SEC=1.0
set TOP_N=10
set SHARDS=8
set QUEUE_SIZE=100000

set TICK_WRITE_URL=http://localhost:8010/write
set TICK_WRITE_SERVER_IP=localhost
set TICK_WRITE_PPA_MAP={"477":2397,"478":2398}

sttp_latency_calculator.exe
pause
