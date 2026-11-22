@echo off
cd /d "%~dp0"

echo ================================================
echo  Quant-Korea-Data Local Runner
echo ================================================
echo.
echo Select mode:
echo   1. Bootstrap 1Y
echo   2. Bootstrap 3Y
echo   3. Bootstrap 5Y
echo   4. Bootstrap MAX (2010~)
echo   5. Dry-run test (no save)
echo.
set /p choice="Enter number: "

if "%choice%"=="1" py -3.12 main.py --mode bootstrap --years-ago 1 --upload-drive
if "%choice%"=="2" py -3.12 main.py --mode bootstrap --years-range 3 --upload-drive
if "%choice%"=="3" py -3.12 main.py --mode bootstrap --years-range 5 --upload-drive
if "%choice%"=="4" py -3.12 main.py --mode bootstrap --year-start 2010 --upload-drive
if "%choice%"=="5" py -3.12 main.py --mode bootstrap --years-ago 1 --dry-run --upload-drive

echo.
pause
