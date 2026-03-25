@echo off
REM Firemni proxy (MKP) — z PAC wpad.mlp.cz vyplyva PROXY 192.168.192.195:8080
REM Pip a Python pouziji HTTP_PROXY / HTTPS_PROXY v tomto CMD sezeni.
set "HTTP_PROXY=http://192.168.192.195:8080"
set "HTTPS_PROXY=http://192.168.192.195:8080"
