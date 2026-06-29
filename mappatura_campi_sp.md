# Mappatura campi output vs Stored Procedure

Data generazione: 2026-06-03

Questo documento descrive la mappatura usata per allineare i nomi campo in output ai nomi attesi dalle stored procedure BOARD/BC.

## Regole generali di rinomina

- La mappatura viene applicata per servizio.
- Per ogni campo, il motore riconosce come input:
  - Nome originale (es. ItemNo)
  - Variante con prefisso FOR_ (es. FOR_ItemNo)
  - Variante Id/ID dove applicabile (es. DimensionSetId vs DimensionSetID)
- Se resta un campo con prefisso FOR_ non mappato esplicitamente, viene rinominato rimuovendo FOR_.

## Tabella di mappatura

| Servizio | Campo originale | Campo stored procedure |
|---|---|---|
| Vendor | No | No_ |
| Vendor | SearchName | Search Name |
| Vendor | Name2 | Name 2 |
| Vendor | GlobalDimension1Code | Global Dimension 1 Code |
| Vendor | GlobalDimension2Code | Global Dimension 2 Code |
| Vendor | CurrencyCode | Currency Code |
| Vendor | VATRegistrationNo | VAT Registration No_ |
| Item | No | No_ |
| Item | ItemCategoryCode | Item Category Code |
| ItemCategory | Code | Code |
| ValueEntryQry | ItemNo | Item No_ |
| ValueEntryQry | PostingDate | Posting Date |
| ValueEntryQry | LocationCode | Location Code |
| ValueEntryQry | UnitofMeasureCode | Unit of Measure Code |
| ValueEntryQry | CostAmountActual | Cost Amount |
| ValueEntryQry | ItemLedgerEntryType | Item Ledger Entry Type |
| ValueEntryQry | DocumentNo | Document No_ |
| PhysInventoryLedgerEntry | ItemNo | Item No_ |
| PhysInventoryLedgerEntry | PostingDate | Posting Date |
| PhysInventoryLedgerEntry | LocationCode | Location Code |
| PhysInventoryLedgerEntry | EntryType | Entry Type |
| PhysInventoryLedgerEntry | DocumentNo | Document No_ |
| PhysInventoryLedgerEntry | Quantity | Qta |
| PhysInventoryLedgerEntry | UnitofMeasureCode | Unit of Measure Code |
| Location | Code | Code |
| Location | Name | Name |
| DimensionSetEntryQry | DimensionSetID | Dimension Set ID |
| DimensionSetEntryQry | DimensionCode | Dimension Code |
| DimensionSetEntryQry | DimensionValueCode | Dimension Value Code |
| DimensionSetEntryQry | DimensionValueID | Dimension Value ID |
| DimensionValue | Code | Code |
| DimensionValue | Name | Name |
| GLAccountQry | No | No_ |
| GLAccountQry | IncomeBalance | Income_Balance |
| GLAccountQry | AccountType | Account Type |
| GLEntry | PostingDate | Posting Date |
| GLEntry | EntryNo | Entry No_ |
| GLEntry | DimensionSetID | Dimension Set ID |
| GLEntry | GlobalDimension2Code | Global Dimension 2 Code |
| GLEntry | GlobalDimension1Code | Global Dimension 1 Code |
| GLEntry | GLAccountNo | G_L Account No_ |
| GLEntry | DocumentNo | Document No_ |
| GLEntry | SourceType | Source Type |
| GLEntry | SourceNo | Source No_ |
| GLEntry | DocumentDate | Document Date |
| GLEntry | DocumentType | Document Type |
| GLEntry | SourceCode | Source Code |
| GLEntry | ExternalDocumentNo | External Document No_ |

## Riferimenti implementativi

- Mappatura usata nel processo principale: new_py_incr - log.py
- Mappatura usata nel post-process parquet->txt: parquet_to_txt_postprocess.py
