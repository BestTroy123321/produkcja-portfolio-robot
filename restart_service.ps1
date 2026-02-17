# Skrypt restartujacy usluge Windows dla SubiektConnector
# Uruchom ten skrypt jako Administrator (Run as Administrator)

$serviceName = "ProdukcjaPortfolioSubiektConnectorService"

Write-Host "Restartowanie uslugi: $serviceName..." -ForegroundColor Cyan

try {
    Restart-Service -Name $serviceName -Force -ErrorAction Stop
    Write-Host "Usluga zostala pomyslnie zrestartowana." -ForegroundColor Green
    
    # Sprawdz status
    $status = Get-Service -Name $serviceName
    Write-Host "Aktualny status: $($status.Status)" -ForegroundColor Yellow
}
catch {
    Write-Host "Blad podczas restartowania uslugi: $_" -ForegroundColor Red
    Write-Host "Upewnij sie, ze uruchamiasz skrypt jako Administrator."
}

Start-Sleep -Seconds 3