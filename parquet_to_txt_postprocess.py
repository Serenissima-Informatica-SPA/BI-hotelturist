import os
import sys
import json
from datetime import datetime

import pandas as pd


SERVICE_FIELD_MAPS_SP = {
    'Vendor': {
        'No': 'No_',
        'Name': 'Name',
        'SearchName': 'Search Name',
        'Name2': 'Name 2',
        'City': 'City',
        'GlobalDimension1Code': 'Global Dimension 1 Code',
        'GlobalDimension2Code': 'Global Dimension 2 Code',
        'CurrencyCode': 'Currency Code',
        'VATRegistrationNo': 'VAT Registration No_',
    },
    'Item': {
        'No': 'No_',
        'ItemNo': 'No_',
        'ItemDescription': 'Description',
        'ItemCategoryCode': 'Item Category Code',
    },
    'ItemCategory': {
        'Code': 'Code',
        'Description': 'Description',
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
    'Location': {
        'Code': 'Code',
        'Name': 'Name',
    },
    'DimensionSetEntryQry': {
        'DimensionSetID': 'Dimension Set ID',
        'DimensionCode': 'Dimension Code',
        'DimensionValueCode': 'Dimension Value Code',
        'DimensionValueID': 'Dimension Value ID',
        'DimensionValueName': 'Name',
    },
    'DimensionValue': {
        'Code': 'Code',
        'Name': 'Name',
    },
    'GLAccountQry': {
        'No': 'No_',
        'Name': 'Name',
        'IncomeBalance': 'Income_Balance',
        'AccountType': 'Account Type',
    },
    'GLEntry': {
        'PostingDate': 'Posting Date',
        'EntryNo': 'Entry No_',
        'Description': 'Description',
        'DimensionSetID': 'Dimension Set ID',
        'GlobalDimension2Code': 'Global Dimension 2 Code',
        'GlobalDimension1Code': 'Global Dimension 1 Code',
        'GLAccountNo': 'G_L Account No_',
        'Amount': 'Amount',
        'DocumentNo': 'Document No_',
        'SourceType': 'Source Type',
        'SourceNo': 'Source No_',
        'DocumentDate': 'Document Date',
        'DocumentType': 'Document Type',
        'SourceCode': 'Source Code',
        'ExternalDocumentNo': 'External Document No_',
    },
}

KEEP_ONLY_MAPPED_FIELDS = True


def _to_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _find_source_column(columns, canonical_name):
    lower_map = {col.lower(): col for col in columns}

    base_variants = [
        canonical_name,
        canonical_name.replace('ID', 'Id'),
    ]
    candidates = []
    for variant in base_variants:
        candidates.append(variant)
        candidates.append(f'FOR_{variant}')

    for candidate in candidates:
        matched = lower_map.get(candidate.lower())
        if matched:
            return matched

    # Gestisce colonne prefissate dal servizio, es. ITM_ItemNo, VE_PostingDate, GLE_GLAccountNo.
    for col in columns:
        col_lower = col.lower()
        for candidate in candidates:
            if col_lower.endswith('_' + candidate.lower()):
                return col

    return None


def _find_tzsi_amount_column(columns):
    explicit = _find_column_by_candidates(
        columns,
        [
            'TZSI_Amount_GL_And_Extra_GL',
            'TZSI Amount G/L And Extra G/L',
            'TZSIAmountGLAndExtraGL',
            'TZSI_Amount_G_L_And_Extra_G_L',
        ]
    )
    if explicit:
        return explicit

    for col in columns:
        key = _normalize_column_key(col)
        if 'tzsi' in key and 'amount' in key and 'extra' in key and 'gl' in key:
            return col
    return None


def _find_tzsi_extra_column(columns):
    explicit = _find_column_by_candidates(
        columns,
        [
            'TZSI_Extra_GL',
            'TZSI Extra G/L',
            'TZSIExtraGL',
            'TZSI_Extra_G_L',
        ]
    )
    if explicit:
        return explicit

    for col in columns:
        key = _normalize_column_key(col)
        if 'tzsi' in key and 'extra' in key and 'gl' in key and 'amount' not in key:
            return col
    return None


def _find_amount_target_column(columns):
    # Evita match ambiguo su colonne come DebitAmount/CreditAmount.
    lower_map = {str(col).lower(): col for col in columns}
    if 'amount' in lower_map:
        return lower_map['amount']
    if 'for_amount' in lower_map:
        return lower_map['for_amount']
    return _find_source_column(columns, 'Amount')


def _normalize_column_key(name):
    raw = ''.join(ch.lower() for ch in str(name) if ch.isalnum())

    # Alcuni nomi OData possono contenere token encoded come x0020/x002f.
    # Li rimuoviamo per confronti piu' robusti.
    out = []
    i = 0
    hexdigits = set('0123456789abcdef')
    while i < len(raw):
        if i + 4 < len(raw) and raw[i] == 'x' and all(ch in hexdigits for ch in raw[i + 1:i + 5]):
            i += 5
            continue
        out.append(raw[i])
        i += 1

    return ''.join(out)


def _find_column_by_candidates(columns, candidates):
    if not columns:
        return None

    lower_map = {str(col).lower(): col for col in columns}
    normalized_map = {_normalize_column_key(col): col for col in columns}

    for candidate in candidates:
        if candidate is None:
            continue

        candidate_str = str(candidate)
        exact = lower_map.get(candidate_str.lower())
        if exact:
            return exact

        normalized = normalized_map.get(_normalize_column_key(candidate_str))
        if normalized:
            return normalized

    normalized_candidates = {_normalize_column_key(candidate) for candidate in candidates if candidate is not None}
    for col in columns:
        col_key = _normalize_column_key(col)
        if not col_key:
            continue
        for candidate_key in normalized_candidates:
            if col_key.endswith(candidate_key):
                return col

    return None


def _normalize_glentry_custom_fields(df, service_name):
    if df is None or df.empty or service_name != 'GLEntry':
        return df

    df_out = df.copy()
    columns = list(df_out.columns)

    amount_col = _find_amount_target_column(columns)
    tzsi_amount_col = _find_tzsi_amount_column(columns)

    # Mantiene il nome finale Amount, sostituendo solo i valori.
    if tzsi_amount_col:
        if amount_col and amount_col != tzsi_amount_col:
            df_out[amount_col] = df_out[tzsi_amount_col]
        elif not amount_col:
            df_out['Amount'] = df_out[tzsi_amount_col]

    columns = list(df_out.columns)
    tzsi_extra_col = _find_tzsi_extra_column(columns)

    if tzsi_extra_col:
        source_code_col = _find_source_column(columns, 'SourceCode')
        if not source_code_col:
            source_code_col = 'SourceCode'
            df_out[source_code_col] = ''

        extra_true_mask = df_out[tzsi_extra_col].apply(lambda v: _to_bool(v, False))
        if extra_true_mask.any():
            df_out.loc[extra_true_mask, source_code_col] = 'NOGIORN'

    return df_out


def _apply_sp_field_mapping(df, service_name):
    if df is None or df.empty:
        return df

    mapping = SERVICE_FIELD_MAPS_SP.get(service_name, {})
    if not mapping:
        return df

    rename_map = {}
    columns = list(df.columns)
    occupied_targets = {col.lower() for col in columns}
    used_sources = set()

    for canonical_name, target_name in mapping.items():
        if target_name.lower() in occupied_targets:
            continue

        source_col = _find_source_column(columns, canonical_name)
        if source_col and source_col not in used_sources:
            rename_map[source_col] = target_name
            used_sources.add(source_col)
            occupied_targets.add(target_name.lower())

    # Fallback generico: rimuove prefisso FOR_ quando il nome pulito non esiste.
    for col in columns:
        if col.lower().startswith('for_') and col not in rename_map:
            clean = col[4:]
            clean_lower = clean.lower()
            if clean and clean_lower not in occupied_targets:
                rename_map[col] = clean
                occupied_targets.add(clean_lower)

    df_out = df.rename(columns=rename_map) if rename_map else df

    if service_name == 'GLEntry' and df is not None and not df.empty:
        source_columns = list(df.columns)
        tzsi_amount_col = _find_tzsi_amount_column(source_columns)
        if tzsi_amount_col:
            amount_target_col = _find_amount_target_column(list(df_out.columns))
            if amount_target_col:
                df_out[amount_target_col] = df[tzsi_amount_col].values

        tzsi_extra_col = _find_tzsi_extra_column(source_columns)
        if tzsi_extra_col is not None:
            source_code_target_col = None
            if 'Source Code' in df_out.columns:
                source_code_target_col = 'Source Code'
            elif 'SourceCode' in df_out.columns:
                source_code_target_col = 'SourceCode'
            else:
                source_code_target_col = _find_source_column(list(df_out.columns), 'SourceCode')

            if source_code_target_col:
                extra_true_mask = df[tzsi_extra_col].apply(lambda v: _to_bool(v, False)).values
                if extra_true_mask.any():
                    df_out.loc[extra_true_mask, source_code_target_col] = 'NOGIORN'

    if not KEEP_ONLY_MAPPED_FIELDS:
        return df_out

    ordered_targets = []
    current_columns = {col.lower(): col for col in df_out.columns}
    for target_name in mapping.values():
        actual = current_columns.get(target_name.lower())
        if actual and actual not in ordered_targets:
            ordered_targets.append(actual)

    if not ordered_targets:
        return df_out

    if 'ReferenceCompany' in df_out.columns and 'ReferenceCompany' not in ordered_targets:
        ordered_targets.append('ReferenceCompany')

    return df_out[ordered_targets]


def _add_reference_company(df, company_sanitized):
    if df is None:
        return pd.DataFrame()

    df_out = df.copy()
    if 'ReferenceCompany' not in df_out.columns:
        df_out['ReferenceCompany'] = company_sanitized
    return df_out


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


def _should_use_partitioned_source(partition_dir, parquet_path):
    if not os.path.isdir(partition_dir):
        return False

    part_files = [
        os.path.join(partition_dir, name)
        for name in os.listdir(partition_dir)
        if name.lower().endswith('.parquet') and os.path.getsize(os.path.join(partition_dir, name)) > 0
    ]
    if not part_files:
        return False

    # Se il parquet monolitico esiste ed e' piu' recente delle partizioni,
    # preferiamo il monolitico (tipico dopo rebuild full one-shot).
    if os.path.exists(parquet_path) and os.path.getsize(parquet_path) > 0:
        latest_part_mtime = max(os.path.getmtime(path) for path in part_files)
        parquet_mtime = os.path.getmtime(parquet_path)
        if parquet_mtime >= latest_part_mtime:
            return False

    return True


def _write_txt(df, txt_path):
    os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    if df is None or df.empty:
        with open(txt_path, 'w', encoding='utf-8'):
            pass
        return 0

    df.to_csv(txt_path, sep='§', index=False, encoding='utf-8')
    return len(df)


def _convert_single_parquet(parquet_path, txt_path, company_sanitized):
    if not os.path.exists(parquet_path) or os.path.getsize(parquet_path) == 0:
        return False, 0, f'Parquet non trovato o vuoto: {parquet_path}'

    try:
        df = pd.read_parquet(parquet_path)
        df = _add_reference_company(df, company_sanitized)
        service_name = os.path.splitext(os.path.basename(parquet_path))[0]
        df = _normalize_glentry_custom_fields(df, service_name)
        df = _apply_sp_field_mapping(df, service_name)
        rows = _write_txt(df, txt_path)
        return True, rows, f'Convertito {parquet_path} -> {txt_path}'
    except Exception as exc:
        return False, 0, f'Errore conversione {parquet_path}: {exc}'


def _convert_partitioned_parquet(partition_dir, txt_path, company_sanitized):
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
            df_part = _add_reference_company(df_part, company_sanitized)
            df_part = _normalize_glentry_custom_fields(df_part, service_name)
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
                sep='§',
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

            if _should_use_partitioned_source(partition_dir, parquet_path):
                ok, rows, message = _convert_partitioned_parquet(partition_dir, txt_path, company_sanitized)
            else:
                ok, rows, message = _convert_single_parquet(parquet_path, txt_path, company_sanitized)

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
