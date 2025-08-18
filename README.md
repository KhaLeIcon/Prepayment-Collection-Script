# js-compile-project

## Overview

`js-compile-project` is a Node.js utility that:

- Fetches and filters sales order data from SAP CPI endpoints
- Processes items and accounting document details
- Outputs JSON artifacts for auditing
- Generates per-company CSVs of pending invoices
- Posts each invoice record to a `PrepaymentAutomation` CPI endpoint

All file paths are resolved relative to the executable or project root, so it works both in development and as a packaged `.exe`.

## Project Structure
```
js-compile-project
├── src
│   ├── main.js
│   └── GetPrepaymentSOCollectionInvoiceList.js
├── CompanyCodeList.xlxs
├── config.yaml
├── package.json
├── README.md
```

## Files Description
- **src/main.js**: The main entry point of the application. It runs the data collection script and processes CSV files, making HTTP requests based on the data retrieved. While it is running, it will call node GetPrepaymentSOCollectionInvoiceList.js to do its job.
- **src/GetPrepaymentSOCollectionInvoiceList.js**: Contains functions to fetch sales order records from an API, process them, and save the results to JSON files. It handles pagination and filtering of records.
- **package.json**: Configuration file for npm. It lists the project name, version, dependencies, and scripts for building the executable file using pkg.

## Installation

```bash
npm install
```

## Usage

1. Place your `config.yaml` and `CompanyCodeList.xlsx` next to the script or `.exe`.
2. Run the processing script:
   ```bash
   node src/main.js
   ```
   This performs:
   - Data fetch & JSON dumps
   - CSV generation under `./<CompanyCode>/`
   - HTTP POSTs to `PrepaymentAutomation`

## Build

To bundle and package into a standalone Windows executable:

1. **Bundle entire app** into a single JS file:
   ```bash
   npx ncc build src/main.js -o dist
   ```
2. **Create the .exe** with pkg:
   ```bash
   pkg dist/index.js --targets node16-win-x64
   ```
3. **Run**:
   ```bash
   ./index.exe
   ```

---

Feel free to adjust `config.yaml` or replace the `CompanyCodeList.xlsx` without rebuilding.