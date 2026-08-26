# Banking Data Pipeline — Bronze Layer Generator

Synthetic data generator untuk project **Core Banking Data Platform** (portofolio Data Engineer, use case perbankan). Men-simulasikan raw export harian dari sistem core banking: cabang, nasabah, rekening, transaksi, dan pinjaman — lengkap dengan kolom audit (`created_at`, `updated_at`, `deleted_at`) untuk mendukung incremental extraction berbasis watermark di tahap Airflow nanti.

## Struktur

```
banking-data-pipeline/
├── data_generator/
│   ├── base.py                  # BaseGenerator (abstract) — Template Method pattern
│   ├── branch_generator.py      # BranchGenerator
│   ├── customer_generator.py    # CustomerGenerator (+ kolom SCD Type 2)
│   ├── account_generator.py     # AccountGenerator (depends on customer & branch)
│   ├── transaction_generator.py # TransactionGenerator (depends on account)
│   └── loan_generator.py        # LoanGenerator (depends on customer & branch)
├── main.py                      # orchestrator, jalankan ini
├── requirements.txt
└── output/bronze/                # hasil generate (CSV) — dibuat otomatis
```

## Kenapa desainnya begini (OOP)

- `BaseGenerator` adalah abstract class yang punya method `generate_one()` (wajib di-override tiap subclass) dan `generate_many()` (logic loop, sama untuk semua entity) — pola **Template Method**.
- `_audit_timestamps()` dipusatkan di base class supaya logic CDC (created/updated/deleted) konsisten di semua entity, tidak copy-paste.
- `AccountGenerator`, `TransactionGenerator`, `LoanGenerator` menerima `customer_ids`/`branch_ids`/`account_ids` di constructor — supaya foreign key antar tabel selalu valid (tidak ada orphan record).
- Tiap generator terpisah file → gampang ditest satu-satu, dan gampang ditambah entity baru (misal `CardGenerator`) tanpa nyentuh yang lain.

## Cara jalankan

```bash
pip install -r requirements.txt
python main.py
```

Output CSV muncul di `output/bronze/`: `branches.csv`, `customers.csv`, `accounts.csv`, `transactions.csv`, `loans.csv`.

Volume data bisa diubah di `main.py` (dict `VOLUMES`). Seed di-fix (`SEED = 42`) supaya dataset reproducible tiap kali di-generate ulang.

## Batch extraction layer (Airflow)

Master/reference data (branches, customers, accounts, loans) di-extract **batch harian** dari source system pakai watermark-based incremental extraction — bukan streaming, karena data ini tidak berubah tiap detik (mirip pola core banking yang sync data referensi semalam/EOD).

```
source_db/
├── seed_source_db.py      # simulasi source OLTP (SQLite) -- initial load + simulate daily changes
watermark/
├── watermark_store.py     # WatermarkStore: simpan last-extracted timestamp per tabel
extractors/
├── base_extractor.py      # BaseExtractor (abstract) -- Template Method, sama pola dengan data_generator
└── concrete_extractors.py # BranchExtractor, CustomerExtractor, AccountExtractor, LoanExtractor
dags/
└── bronze_batch_extraction_dag.py  # Airflow DAG, jadwal harian 01:00
```

**Cara test tanpa Airflow (logic murni Python):**

```bash
python -m source_db.seed_source_db                    # initial load ke source_db/core_banking.db
python -c "
from extractors.concrete_extractors import BranchExtractor, CustomerExtractor, AccountExtractor, LoanExtractor
for cls in [BranchExtractor, CustomerExtractor, AccountExtractor, LoanExtractor]:
    print(cls().extract())
"
# simulasikan hari berikutnya (beberapa row berubah, beberapa row baru)
python -m source_db.seed_source_db --simulate-daily-changes
# jalankan lagi -- hanya delta yang ke-extract
```

**Cara pakai di Airflow beneran:** copy folder `dags/`, `extractors/`, `watermark/`, `source_db/`, `data_generator/` ke environment Airflow (mount seperti setup Docker Compose bootcamp kamu), pastikan project root ada di `PYTHONPATH`.

## Kenapa transaksi TIDAK ada di DAG ini

Transaksi (mobile banking, ATM, dll) sifatnya high-volume dan event-driven — nunggu jadwal batch harian gak realistis buat use case fraud detection/monitoring saldo real-time. Itu di-handle terpisah lewat **Kafka streaming** (belum dibangun di repo ini — langkah selanjutnya).

## Langkah selanjutnya

1. **Kafka**: producer simulasi transaksi real-time + consumer yang landing ke Bronze (partitioned by day)
2. **Silver layer**: PII masking (hash `nik`), data quality checks (referential integrity, rekonsiliasi debit=kredit)
3. **Gold layer**: star schema untuk dashboard NPL & customer segmentation
