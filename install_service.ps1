# Skrypt instalujacy usluge Windows dla SubiektConnector
# Uruchom ten skrypt jako Administrator (Run as Administrator)

$serviceName = "ProdukcjaPortfolioSubiektConnectorService"
$displayName = "Produkcja Portfolio Subiekt Connector"
$description = "Automatyczna synchronizacja Subiekt GT -> Webhook (ZD i RW)"
$exePath = Join-Path $PSScriptRoot "bin\Debug\net48\SubiektConnector.exe"

# Sprawdz czy plik exe istnieje
if (-not (Test-Path $exePath)) {
    Write-Host "Blad: Nie znaleziono pliku wykonywalnego w: $exePath" -ForegroundColor Red
    Write-Host "Upewnij sie, ze projekt zostal zbudowany (dotnet build)."
    exit 1
}

Write-Host "Instalacja uslugi: $serviceName" -ForegroundColor Cyan
Write-Host "Sciezka do pliku: $exePath"

# Zatrzymaj usluge jesli istnieje
if (Get-Service $serviceName -ErrorAction SilentlyContinue) {
    Write-Host "Zatrzymywanie istniejacej uslugi..."
    Stop-Service $serviceName -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    
    Write-Host "Usuwanie istniejacej uslugi..."
    sc.exe delete $serviceName | Out-Null
    Start-Sleep -Seconds 2
}

# Utworz nowa usluge
# Uzywamy sc.exe dla precyzyjnej kontroli (szczegolnie binPath ze spacjami)
$binPath = "`"$exePath`""
Write-Host "Tworzenie uslugi..."
$result = sc.exe create $serviceName binPath= $binPath start= auto DisplayName= $displayName
if ($LASTEXITCODE -ne 0) {
    Write-Host "Blad podczas tworzenia uslugi. Sprawdz uprawnienia administratora." -ForegroundColor Red
    exit $LASTEXITCODE
}

# Ustaw opis
sc.exe description $serviceName $description | Out-Null

# Ustawienie restartu po awarii (opcjonalne, ale zalecane)
sc.exe failure $serviceName reset= 86400 actions= restart/60000/restart/60000/restart/60000 | Out-Null

Write-Host "Uruchamianie uslugi..."
Start-Service $serviceName

Write-Host "Zakonczono pomyslnie!" -ForegroundColor Green
Write-Host "Usluga '$displayName' zostala zainstalowana i uruchomiona."
Write-Host "Bedzie uruchamiac sie automatycznie po starcie systemu."
