$projectPath = "D:\Trae\Michał Kuriata\Produkcja portfolio - robot C#"
$command = "cd `"$projectPath`"; dotnet run"
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $command
