echo off
set local_dir=%cd%
set cur_dir=%~dp0

:: find vcvarsall.bat
setlocal ENABLEDELAYEDEXPANSION
"%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath
for /F "tokens=*" %%i in ('"%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe" -latest -property installationPath') do ( set vsinstallationPath=%%i)
echo vsinstallationPath=%vsinstallationPath%
set vcvarsallPath=%vsinstallationPath%\VC\Auxiliary\Build
echo vcvarsallPath=%vcvarsallPath%
set vcvarsall=%vcvarsallPath%\vcvarsall.bat
cd /d %vcvarsallPath%
set VSCMD_DEBUG=0
where cl.exe
if %ERRORLEVEL% == 1 call vcvarsall.bat x86
cd /d %cur_dir%

:: build
call nmake /nologo /f Makefile.namke clean
nmake /nologo  /f Makefile.namke

cd /d %local_dir%
