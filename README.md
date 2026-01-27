# SubiektConnector

## Wymagania

- .NET Framework 4.8
- .NET SDK (do `dotnet restore`)
- Dostęp do bazy MS SQL Subiekt GT

## Instalacja

1. Sklonuj repozytorium.
2. Skopiuj `App.config.template` do `App.config`.
3. Uzupełnij `App.config`:
   - `N8nUrl`
   - `SubiektDB`
4. Pobierz paczki NuGet:
   ```powershell
   dotnet restore
   ```

## Uruchomienie

```powershell
dotnet run
```
