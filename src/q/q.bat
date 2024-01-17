@echo off

setlocal
if exist "%~dp0..\python.exe" ( "%~dp0..\python" "%~dp0q" %* ) else ( python "%~dp0q" %* )
endlocal
