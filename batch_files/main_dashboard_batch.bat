setlocal
set PYTHONPATH=%USERPROFILE%\open-data-api
call "C:\Users\meurost\AppData\Local\Continuum\anaconda3\Scripts\activate.bat" auckland-index & cd "C:\Users\meurost\Auckland-Index-Update" & python "auckland_index_update_main.py"
IF %ERRORLEVEL% NEQ 0 PAUSE
endlocal