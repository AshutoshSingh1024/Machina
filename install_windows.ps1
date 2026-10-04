$ErrorActionPreference = "Stop"

Write-Host "Methodology: install a usable Python runtime if missing, install project Python packages, then install Apache Flink globally under C:\flink."

$pythonOk = $false
try {
    py -3.11 --version | Out-Host
    $pythonOk = $true
} catch {
    $pythonOk = $false
}

if (-not $pythonOk) {
    $pythonInstaller = "$env:TEMP\python-3.11.9-amd64.exe"
    Write-Host "Installing Python 3.11 because PyFlink requires Python 3.11 or older here..."
    Invoke-WebRequest -Uri "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe" -OutFile $pythonInstaller
    Start-Process -FilePath $pythonInstaller -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait
    $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")
}

py -3.11 -m ensurepip --upgrade
py -3.11 -m pip install --upgrade pip
$env:PIP_CONSTRAINT = "$PWD\constraints.txt"
py -3.11 -m pip install -r requirements.txt
Remove-Item Env:\PIP_CONSTRAINT -ErrorAction SilentlyContinue

$flinkVersion = "1.19.1"
$scalaVersion = "2.12"
$installRoot = "C:\flink"
$archive = "$env:TEMP\flink-$flinkVersion-bin-scala_$scalaVersion.tgz"
$url = "https://archive.apache.org/dist/flink/flink-$flinkVersion/flink-$flinkVersion-bin-scala_$scalaVersion.tgz"

if (-not (Test-Path $installRoot)) {
    Write-Host "Downloading Apache Flink $flinkVersion..."
    Invoke-WebRequest -Uri $url -OutFile $archive
    New-Item -ItemType Directory -Path $installRoot | Out-Null
    tar -xzf $archive -C $installRoot --strip-components=1
}

$currentPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
if ($currentPath -notlike "*$installRoot\bin*") {
    [Environment]::SetEnvironmentVariable("Path", "$currentPath;$installRoot\bin", "Machine")
    Write-Host "Added C:\flink\bin to the Machine PATH. Open a new terminal before using flink commands."
}

Write-Host "Done. Verify with: flink --version"
