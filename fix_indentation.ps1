# PowerShell script per riparare l'indentazione

$filePath = "c:\Serenissima\BI-hotelturist\new_py_incr - log.py"

# Leggi il file
$content = Get-Content $filePath -Encoding UTF8 -Raw
$lines = $content -split "`n"

$tryLineNum = -1
$exceptLineNum = -1

# Cerca le linee di try e except
for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($i -ge 2100 -and $i -le 2110) {
        if ($lines[$i] -match '^\s+try:') {
            $tryLineNum = $i
            Write-Host "Found 'try:' at line $($i + 1)"
        }
    }
    
    if ($i -ge 3688 -and $i -le 3698) {
        if ($lines[$i] -match '^\s+except Exception as e:') {
            $exceptLineNum = $i
            Write-Host "Found 'except Exception as e:' at line $($i + 1)"
        }
    }
}

if ($tryLineNum -eq -1 -or $exceptLineNum -eq -1) {
    Write-Host "ERROR: Could not find try/except markers!"
    Write-Host "tryLineNum: $tryLineNum, exceptLineNum: $exceptLineNum"
    exit 1
}

Write-Host "`nIndenting lines from $($tryLineNum + 2) to $exceptLineNum"

# Reindent le linee nel mezzo
for ($i = $tryLineNum + 1; $i -lt $exceptLineNum; $i++) {
    $line = $lines[$i]
    
    # Aggiungi 4 spazi se la riga non è vuota
    if ($line.Trim()) {
        $lines[$i] = "    " + $line
    }
}

# Scrivi il file modificato
$newContent = $lines -join "`n"
Set-Content -Path $filePath -Value $newContent -Encoding UTF8

Write-Host "File reindentato con successo!"
