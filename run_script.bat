@ECHO OFF
setlocal
echo current path: %~dp0
set PYTHONPATH=%~dp0
python %1
endlocal
