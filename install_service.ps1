# Skrypt instalujący usługę Windows dla SubiektConnector
# Uruchom ten skrypt jako Administrator (Run as Administrator)

$serviceName = "ProdukcjaPortfolioSubiektConnectorService"
$displayName = "Produkcja Portfolio Subiekt Connector"
$description = "Automatyczna synchronizacja Subiekt GT -> Webhook (ZD i RW)"
$exePath = Join-Path $PSScriptRoot "bin\Debug\net48\SubiektConnector.exe"

# Sprawdź czy plik exe istnieje
if (-not (Test-Path $exePath)) {
    Write-Host "Błąd: Nie znaleziono pliku wykonywalnego w: $exePath" -ForegroundColor Red
    Write-Host "Upewnij się, że projekt został zbudowany (dotnet build)."
    exit 1
}

Write-Host "Instalacja usługi: $serviceName" -ForegroundColor Cyan
Write-Host "Ścieżka do pliku: $exePath"

# Zatrzymaj usługę jeśli istnieje
if (Get-Service $serviceName -ErrorAction SilentlyContinue) {
    Write-Host "Zatrzymywanie istniejącej usługi..."
    Stop-Service $serviceName -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    
    Write-Host "Usuwanie istniejącej usługi..."
    sc.exe delete $serviceName | Out-Null
    Start-Sleep -Seconds 2
}

# Utwórz nową usługę
# Używamy sc.exe dla precyzyjnej kontroli (szczególnie binPath ze spacjami)
$binPath = "`"$exePath`""
Write-Host "Tworzenie usługi..."
$result = sc.exe create $serviceName binPath= $binPath start= auto DisplayName= $displayName
if ($LASTEXITCODE -ne 0) {
    Write-Host "Błąd podczas tworzenia usługi. Sprawdź uprawnienia administratora." -ForegroundColor Red
    exit $LASTEXITCODE
}

# Ustaw opis
sc.exe description $serviceName $description | Out-Null

# Ustawienie restartu po awarii (opcjonalne, ale zalecane)
sc.exe failure $serviceName reset= 86400 actions= restart/60000/restart/60000/restart/60000 | Out-Null

Write-Host "Uruchamianie usługi..."
Start-Service $serviceName

Write-Host "Zakończono pomyślnie!" -ForegroundColor Green
Write-Host "Usługa '$displayName' została zainstalowana i uruchomiona."
Write-Host "Będzie uruchamiać się automatycznie po starcie systemu."
