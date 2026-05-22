import os
import sys
import json
from datetime import datetime

import pandas as pd


SERVICE_FIELD_MAPS_SP = {
    'Vendor': {
        'No': 'No_',
        'SearchName': 'Search Name',
        'Name2': 'Name 2',
        'GlobalDimension1Code': 'Global Dimension 1 Code',
        'GlobalDimension2Code': 'Global Dimension 2 Code',
        'CurrencyCode': 'Currency Code',
        'VATRegistrationNo': 'VAT Registration No_',
    },
    'Item': {
        'No': 'No_',
        'ItemCategoryCode': 'Item Category Code',
    },
    'ValueEntryQry': {
        'ItemNo': 'Item No_',
        'PostingDate': 'Posting Date',
        'LocationCode': 'Location Code',
        'UnitofMeasureCode': 'Unit of Measure Code',
        'CostAmountActual': 'Cost Amount',
        'ItemLedgerEntryType': 'Item Ledger Entry Type',
        'DocumentNo': 'Document No_',
    },
    'PhysInventoryLedgerEntry': {
        'ItemNo': 'Item No_',
        'PostingDate': 'Posting Date',
        'LocationCode': 'Location Code',
        'EntryType': 'Entry Type',
        'DocumentNo': 'Document No_',
        'Quantity': 'Qta',
        'UnitofMeasureCode': 'Unit of Measure Code',
    },
    'DimensionSetEntryQry': {
        'DimensionSetID': 'Dimension Set ID',
        'DimensionCode': 'Dimension Code',
        'DimensionValueCode': 'Dimension Value Code',
        'DimensionValueID': 'Dimension Value ID',
    },
    'GLAccountQry': {
        'No': 'No_',
        'IncomeBalance': 'Income_Balance',
        'AccountType': 'Account Type',
    },
    'GLEntry': {
        'PostingDate': 'Posting Date',
        'EntryNo': 'Entry No_',
        'DimensionSetID': 'Dimension Set ID',
        'GlobalDimension2Code': 'Global Dimension 2 Code',
        'GlobalDimension1Code': 'Global Dimension 1 Code',
        'GLAccountNo': 'G_L Account No_',
        'DocumentNo': 'Document No_',
        'SourceType': 'Source Type',
        'SourceNo': 'Source No_',
        'DocumentDate': 'Document Date',
        'DocumentType': 'Document Type',
        'SourceCode': 'Source Code',
        'ExternalDocumentNo': 'External Document No_',
    },
}


def _apply_sp_field_mapping(df, service_name):
    if df is None or df.empty:
        return df

    mapping = SERVICE_FIELD_MAPS_SP.get(service_name, {})
    if not mapping:
        return df

    rename_map = {}
    columns = set(df.columns)
    for canonical_name, target_name in mapping.items():
        candidates = [
            canonical_name,
            f'FOR_{canonical_name}',
            canonical_name.replace('ID', 'Id'),
            f"FOR_{canonical_name.replace('ID', 'Id')}",
        ]
        for candidate in candidates:
            if candidate in columns and target_name not in columns:
                rename_map[candidate] = target_name
                break

    if not rename_map:
        # Fallback generico: rimuove prefisso FOR_ quando il nome pulito non esiste.
        for col in list(columns):
            if col.startswith('FOR_'):
                clean = col[4:]
                if clean and clean not in columns:
                    rename_map[col] = clean

    if not rename_map:
        return df
    return df.rename(columns=rename_map)


def trova_file_configurazione():
    candidate_dirs = []

    if getattr(sys, 'frozen', False):
        candidate_dirs.append(os.path.dirname(sys.executable))

    if '__file__' in globals():
        candidate_dirs.append(os.path.dirname(os.path.abspath(__file__)))

    candidate_dirs.append(os.getcwd())

    if len(sys.argv) > 0 and sys.argv[0]:
        candidate_dirs.append(os.path.dirname(os.path.abspath(sys.argv[0])))

    dirs_uniche = []
    for directory in candidate_dirs:
        if directory and directory not in dirs_uniche:
            dirs_uniche.append(directory)

    attempted_paths = []
    for directory in dirs_uniche:
        candidate = os.path.join(directory, 'configuration.json')
        attempted_paths.append(candidate)
        if os.path.isfile(candidate):
            return candidate

    raise FileNotFoundError(
        'configuration.json non trovato. Percorsi tentati: ' + ' | '.join(attempted_paths)
    )


def _service_folder_name(service_name, company_sanitized):
    return f"{service_name.replace('Qry', '').replace('_', '').lower()}-{company_sanitized}"


def _write_txt(df, txt_path):
    os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    if df is None or df.empty:
        with open(txt_path, 'w', encoding='utf-8'):
            pass
        return 0

    df.to_csv(txt_path, sep='\t', index=False, encoding='utf-8')
    return len(df)


def _convert_single_parquet(parquet_path, txt_path):
    if not os.path.exists(parquet_path) or os.path.getsize(parquet_path) == 0:
        return False, 0, f'Parquet non trovato o vuoto: {parquet_path}'

    try:
        df = pd.read_parquet(parquet_path)
        service_name = os.path.splitext(os.path.basename(parquet_path))[0]
        df = _apply_sp_field_mapping(df, service_name)
        rows = _write_txt(df, txt_path)
        return True, rows, f'Convertito {parquet_path} -> {txt_path}'
    except Exception as exc:
        return False, 0, f'Errore conversione {parquet_path}: {exc}'


def _convert_partitioned_parquet(partition_dir, txt_path):
    if not os.path.isdir(partition_dir):
        return False, 0, f'Cartella partizioni non trovata: {partition_dir}'

    parquet_files = [
        os.path.join(partition_dir, name)
        for name in sorted(os.listdir(partition_dir))
        if name.lower().endswith('.parquet')
    ]

    if not parquet_files:
        return False, 0, f'Nessun file parquet in {partition_dir}'

    total_rows = 0
    first_write = True
    expected_columns = None

    try:
        os.makedirs(os.path.dirname(txt_path), exist_ok=True)
        service_name = os.path.basename(txt_path).replace('.txt', '')
        for parquet_file in parquet_files:
            if os.path.getsize(parquet_file) == 0:
                continue

            df_part = pd.read_parquet(parquet_file)
            df_part = _apply_sp_field_mapping(df_part, service_name)

            if expected_columns is None:
                expected_columns = list(df_part.columns)
            else:
                for col in expected_columns:
                    if col not in df_part.columns:
                        df_part[col] = pd.NA
                df_part = df_part[expected_columns]

            if df_part.empty:
                continue

            df_part.to_csv(
                txt_path,
                sep='\t',
                index=False,
                encoding='utf-8',
                mode='w' if first_write else 'a',
                header=first_write,
            )
            total_rows += len(df_part)
            first_write = False

        if first_write:
            with open(txt_path, 'w', encoding='utf-8'):
                pass

        return True, total_rows, f'Convertite partizioni {partition_dir} -> {txt_path}'
    except Exception as exc:
        return False, total_rows, f'Errore conversione partizionata {partition_dir}: {exc}'


def main():
    config_path = trova_file_configurazione()
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    local_output_path = config.get('local_output_path', r'C:\BC-BOARD')
    companies = config.get('companies', [])
    services_mov = config.get('defServ_mov', [])
    services_anag = config.get('defServ_anag', [])

    if not companies:
        raise ValueError('Nessuna company configurata in configuration.json')

    services = []
    for name in services_mov + services_anag:
        if name and name not in services:
            services.append(name)

    total_ok = 0
    total_fail = 0
    details = []

    start = datetime.now()
    print(f"[START] Conversione parquet->txt {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Config: {config_path}")
    print(f"Output root: {local_output_path}")

    for company in companies:
        company_sanitized = company.get('sanitized', '').strip()
        if not company_sanitized:
            continue

        for service_name in services:
            folder_name = _service_folder_name(service_name, company_sanitized)
            output_folder = os.path.join(local_output_path, folder_name)
            txt_path = os.path.join(output_folder, f'{service_name}.txt')
            parquet_path = os.path.join(output_folder, f'{service_name}.parquet')
            partition_dir = os.path.join(output_folder, f'{service_name}__parts')

            if os.path.isdir(partition_dir):
                ok, rows, message = _convert_partitioned_parquet(partition_dir, txt_path)
            else:
                ok, rows, message = _convert_single_parquet(parquet_path, txt_path)

            details.append((ok, service_name, company_sanitized, rows, message))
            if ok:
                total_ok += 1
                print(f"[OK] {service_name} ({company_sanitized}) -> {rows} righe")
            else:
                total_fail += 1
                print(f"[SKIP/ERR] {service_name} ({company_sanitized}) -> {message}")

    end = datetime.now()
    elapsed = (end - start).total_seconds()
    print('-' * 80)
    print(f"[END] {end.strftime('%Y-%m-%d %H:%M:%S')} | Durata: {elapsed:.1f}s")
    print(f"Tabelle convertite: {total_ok}")
    print(f"Tabelle non convertite: {total_fail}")


if __name__ == '__main__':
    main()
