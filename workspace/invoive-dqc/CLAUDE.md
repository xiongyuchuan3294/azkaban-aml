# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an **Invoice Processing System** (发票智能筛选系统) for automated employee reimbursement at 深圳前海微众银行股份有限公司. The codebase contains **two separate implementations**:

1. **`invoice_processor/`** - Production-ready system (text-based PDF extraction, Markdown reports)
2. **`invoice_reimbursement/`** - Alternative system (PaddleOCR for images, Excel reports)

Both scan PDF invoices, validate against business rules, and select optimal combinations to maximize monthly subsidy utilization.

## Running the Application

### Primary System (`invoice_processor/`)

```bash
# Navigate to processor directory
cd invoice_processor

# Install dependencies
pip3 install -r requirements.txt

# Basic usage - process invoices for a specific month
python3 main.py --month 2026-01

# Full parameters (from project root)
python3 invoice_processor/main.py --month 2026-01 --invoice-pool invoice --output-dir output
```

**Important:** Use `python3` not `python` (python2 is default on this system).

### Alternative System (`invoice_reimbursement/`)

```bash
cd invoice_reimbursement
pip3 install -r requirements.txt  # Includes PaddleOCR (heavier)
python3 main.py --month 2026-01 --employee "张三"
```

## Architecture

The primary system (`invoice_processor/`) follows a **pipeline pattern** with clear separation of concerns:

```
main.py (CLI)
  ↓
InvoiceProcessor (orchestrator)
  ↓
  ├─→ InvoiceRecognizer (pdfplumber/PyMuPDF text extraction)
  ├─→ InvoiceValidator (business rules)
  ├─→ InvoiceSelector (exhaustive combinatorial search)
  ├─→ FileManager (file operations)
  └─→ ReportGenerator (Markdown output)
```

### Data Flow

1. **Backup**: Incremental copy of new files from `invoice/` to `invoice_bak/`
2. **Load Existing**: Parse already-selected invoices from `output/{month}/` by filename pattern
3. **Scan**: Find all PDF files in invoice pool
4. **Recognize**: Extract invoice data via regex-based text extraction
5. **Duplicate Check**: Track invoice numbers, mark duplicates as ERROR
6. **Validate**: Apply business rules (date range, header, blacklist, types)
7. **Select**: Per-type exhaustive search using `itertools.combinations`
8. **Merge**: Combine existing + new selection results
9. **Process Files**: Move/rename based on status
10. **Generate Report**: Create Markdown report with statistics

### Core Components

| File | Responsibility |
|------|----------------|
| `processor.py` | Main orchestrator; handles backup, merging, duplicate detection |
| `recognizer.py` | PDF text extraction using pdfplumber (primary) or PyMuPDF (fallback) |
| `validator.py` | Business rule validation (dates, headers, blacklist, type classification) |
| `selector.py` | Exhaustive combinatorial optimization per invoice type |
| `file_manager.py` | Moves/renames files based on status (SELECTED/UNUSED/ERROR) |
| `reporter.py` | Generates Markdown reimbursement reports |
| `models.py` | Data models (`InvoiceInfo`, `InvoiceType`, `InvoiceStatus`, `SelectionResult`) |
| `config.py` | Business rules (subsidy limits, keywords, blacklist, company info) |

## Directory Structure

```
invoice/                         # Input: PDF invoice pool
invoice_bak/                     # Backup: Incremental backups (only new files)
invoice_processor/               # Primary implementation
  ├── main.py
  ├── processor.py
  ├── recognizer.py
  ├── validator.py
  ├── selector.py
  ├── file_manager.py
  ├── reporter.py
  ├── models.py
  ├── config.py
  └── README.md                  # Chinese user guide
invoice_reimbursement/           # Alternative implementation (OCR-based)
output/                          # Output base
  ├── 2026-01/                  # Selected invoices for month
  ├── errors/                   # Invalid invoices
  └── 报销报告_2026-01.md        # Markdown report
```

## Business Rules (from config.py)

### Subsidy Limits per Type
- Dining (餐饮): 680.00 CNY
- Transport (交通): 500.00 CNY
- Communication (通讯): 300.00 CNY

### Validation Rules
1. **Date Range**: Invoice must be within 3-month billing period (reimbursement month ± 2 months)
2. **Header**: Buyer name + tax ID must match company (communication allows individual names)
3. **Blacklist**: 8 blocked suppliers (see `BLACKLIST_SUPPLIERS` in config.py)
4. **Duplicate Detection**: Duplicate invoice numbers marked as ERROR
5. **Type Keywords**: Each type has specific keywords (see `INVOICE_TYPES` in config.py)

### Selection Strategy
The selector uses exhaustive search (`itertools.combinations`) prioritizing:
1. Closest to subsidy limit (within 30% overage allowed)
2. Fewer invoices (to save for future months)
3. Smaller amounts when totals are similar
4. FIFO behavior (sort by date then amount)

## File Naming Conventions

The system uses English type names and omits merchant names (to avoid encoding issues):

- **Selected**: `{type}_{amount}_{unique_id}.pdf`
  - Example: `dining_108.91_470541.pdf`
- **Unused** (kept in pool): `[EXTRA]{type}_{amount}_{unique_id}.pdf`
  - Example: `[EXTRA]dining_395.05_438781.pdf`
- **Error** (invalid): `[ERROR-{code}]{type}_{amount}_{unique_id}.pdf`
  - Error codes: `RECOG` (recognition failed), `DUP` (duplicate), `UNKNOWN`
- **Duplicate**: `[ERROR-DUP]{original_filename}.pdf`

**Note**: The filename parser in `processor.py` supports both the new English format and legacy Chinese format (`餐饮_110.00元_销方名称.pdf`) for backward compatibility.

## Key Implementation Details

### Duplicate Invoice Handling
- Detected in `processor.py` via `seen_invoice_numbers` tracking
- Sets `is_duplicate=True` on `InvoiceInfo`
- `selector.py` excludes duplicates from selection, sets `status=ERROR`
- `models.py` generates `[ERROR-DUP]` filename prefix
- `file_manager.py` moves duplicates to `output/errors/`

### Filename Parsing for Incremental Processing
Already-selected invoices are loaded from `output/{month}/` by parsing filename:
- Supports both English (`dining_100.00_123456`) and Chinese (`餐饮_100.00元_销方`) formats
- Extracts type and amount for merge calculation with new selections

### Invoice Type Classification
Based on keywords in "货物或应税劳务、服务名称" field:
- Keywords defined in `config.py` INVOICE_TYPES
- Dining excludes: 充值权益, 会员卡, 预付卡

### Date Range Calculation
Billing period = reimbursement month ± 2 months (implemented via `dateutil.relativedelta`):
- For 2026-01: accepts 2025-11, 2025-12, 2026-01

## Company Information

All from `config.py` COMPANY_INFO:
- Name: 深圳前海微众银行股份有限公司
- Tax ID: 9144030031977063XH
- Bank: 招商银行股份有限公司深圳前海分行
- Account: 755924276310998

## Implementation Comparison

| Feature | invoice_processor | invoice_reimbursement |
|---------|-------------------|----------------------|
| OCR Engine | pdfplumber, PyMuPDF (text) | PaddleOCR, Tesseract (images) |
| Input | PDF only | PDF, JPG, PNG |
| Output | Markdown report | Excel report |
| Selection | Exhaustive combinatorial | Simple validation only |
| Duplicates | Yes | No |
| Incremental Backup | Yes | No |

## Dependencies

**invoice_processor** (lightweight):
```
pdfplumber>=0.10.0      # PDF text extraction (primary)
PyMuPDF>=1.23.0         # PDF text extraction (fallback)
python-dateutil>=2.8.0  # Date calculations
```

**invoice_reimbursement** (heavier, includes PaddleOCR):
```
paddleocr>=2.7.0        # OCR for images
pillow>=10.0.0          # Image processing
pdf2image>=1.16.0       # PDF to image conversion
pandas>=2.0.0           # Excel reports
openpyxl>=3.1.0         # Excel file format
```

## Documentation

- `invoice_processor/README.md` - Chinese user guide
- `invoice_processor/发票处理规则说明.md` - Comprehensive business rules documentation
