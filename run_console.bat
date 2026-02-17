@echo off
set "PROJECT_PATH=C:\Users\Administrator\Desktop\produkcja-portfolio-robot"
start "" powershell -NoExit -Command "cd '%PROJECT_PATH%'; dotnet run"
