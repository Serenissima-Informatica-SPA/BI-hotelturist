#!/usr/bin/env python3
# Script per riparare l'indentazione del try-except nel loop della company

import re

file_path = r"c:\Serenissima\BI-hotelturist\new_py_incr - log.py"

# Leggi il file
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Trova il numero di riga del "try:"
try_line_num = None
except_line_num = None

for i, line in enumerate(lines):
    if i >= 2100 and i <= 2110:  # Cerca intorno a riga 2103
        if re.match(r'^\s+try:', line):
            try_line_num = i
            print(f"Found 'try:' at line {i + 1}")
    
    if i >= 3690 and i <= 3700:  # Cerca intorno a riga 3688
        if re.match(r'^\s+except Exception as e:', line):
            except_line_num = i
            print(f"Found 'except Exception as e:' at line {i + 1}")

if try_line_num is None or except_line_num is None:
    print(f"ERROR: Could not find try/except markers!")
    print(f"try_line_num: {try_line_num}, except_line_num: {except_line_num}")
    exit(1)

print(f"\nIndentando linee da {try_line_num + 2} a {except_line_num}")

# Riindenta le linee nel mezzo
for i in range(try_line_num + 1, except_line_num):
    line = lines[i]
    
    # Aggiungi 4 spazi all'inizio se la riga non è vuota
    if line.strip():  # Se la riga ha contenuto
        lines[i] = "    " + line
    else:  # Se la riga è vuota, mantienila vuota
        lines[i] = line

# Scrivi il file modificato
with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("File reindentato con successo!")
