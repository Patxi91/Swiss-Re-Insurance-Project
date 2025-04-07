# Swiss-Re-Insurance-Project  
Data Transformation Pipeline: Contracts and Claims to Transactions  

---

## Project Overview  

This project implements a **data transformation pipeline** using **Python** and **PySpark** to process contracts and claims data. The pipeline generates transactions that comply with an internal data model, following business logic and technical requirements provided for the "Europe 3" data source.

The pipeline is modular, testable, and extensible, designed for robust batch processing.

---

## Features  

- Converts **contracts** and **claims** datasets into a unified **transactions** dataset.
- Applies domain-specific business logic:
  - Converts `CLAIM_TYPE` into standardized `TRANSACTION_TYPE`.
  - Infers `TRANSACTION_DIRECTION` based on the `CLAIM_ID` prefix.
  - Transforms `CLAIM_ID` to extract `SOURCE_SYSTEM_ID`.
  - Maps `DATE_OF_LOSS`, `CREATION_DATE`, and runtime `SYSTEM_TIMESTAMP`.
  - Uses a REST API (`hashify.net`) to generate unique `NSE_ID` hashes for transaction records.
- Maintains **idempotency** by appending new transactions without overwriting past batches.
- Includes unit tests and data validation steps.

---

## Workflow  

1. **Read and Validate Input**  
   - Load contracts and claims datasets using PySpark.
   - Validate schema structure and column presence.

2. **Join & Transform**  
   - Join claims with contracts via `CONTRACT_ID` and `SOURCE_SYSTEM`.
   - Apply mappings and transformations:
     - `CLAIM_TYPE` → `TRANSACTION_TYPE` ("Corporate", "Private", or "Unknown").
     - `CLAIM_ID` prefix → `TRANSACTION_DIRECTION` ("REINSURANCE" or "COINSURANCE").
     - Strip prefix to obtain `SOURCE_SYSTEM_ID`.
     - Format `DATE_OF_LOSS` to `BUSINESS_DATE`.
     - Generate `SYSTEM_TIMESTAMP` using current datetime.

3. **Enrich with Unique ID**  
   - For each row, call `https://hashify.net` API with MD4 hash of `CLAIM_ID`.
   - Assign result as `NSE_ID`.

4. **Write Output**  
   - Save the resulting transactions DataFrame to file in the specified format (e.g., Parquet or CSV).
   - Output is non-destructive—previous batches remain untouched.

---

## Input Data  

### Contracts  
Schema:  
- **`SOURCE_SYSTEM`**: Text (Primary Key)  
- **`CONTRACT_ID`**: Number (Primary Key)  
- **`CONTRACT_TYPE`**: Text  
- **`INSURED_PERIOD_FROM`**: Date  
- **`INSURED_PERIOD_TO`**: Date  
- **`CREATION_DATE`**: Date  

### Claims  
Schema:  
- **`SOURCE_SYSTEM`**: Text (Primary Key)  
- **`CLAIM_ID`**: Number (Primary Key)  
- **`CONTRACT_SOURCE_SYSTEM`**: Text  
- **`CONTRACT_ID`**: Number (Primary Key)  
- **`CLAIM_TYPE`**: Text  
- **`DATE_OF_LOSS`**: Date  
- **`AMOUNT`**: Decimal (16,5)  
- **`CREATION_DATE`**: Date  

---

## Output Data  

### Transactions  
Schema:  
- **`CONTRACT_SOURCE_SYSTEM`**: String (Primary Key)  
- **`CONTRACT_SOURCE_SYSTEM_ID`**: Long (Primary Key)  
- **`SOURCE_SYSTEM_ID`**: Integer  
- **`TRANSACTION_TYPE`**: String  
- **`TRANSACTION_DIRECTION`**: String  
- **`CONFORMED_VALUE`**: Decimal (16,5)  
- **`BUSINESS_DATE`**: Date  
- **`CREATION_DATE`**: Timestamp  
- **`SYSTEM_TIMESTAMP`**: Timestamp  
- **`NSE_ID`**: String (Primary Key)  

---

## Installation Guide  

### Requirements  

The following Python libraries are required for the implementation:  

- **PySpark (`pyspark>=3.5.0`)**: For distributed data processing and transformation.  
- **Requests (`requests>=2.31.0`)**: To interact with the REST API for generating unique transaction IDs.  

### Installation  

1. Clone the repository:  
   ```bash
   git clone https://github.com/Patxi91/Swiss-Re-Insurance-Project.git  
   cd Swiss-Re-Insurance-Project
2. Clone the repository:  
   ```bash
   pip install -r requirements.txt
### How to Run
1. Run the pipeline: 
   ```bash
   python main.py
### License
Do not share or distribute this code without prior authorization.

##
## Conceptual Proposal: Handling New Batches of Input Data

To prevent overriding existing transactions while processing new batches, we can follow the following steps:

1. **Batch Identification**:
   Assign a unique identifier (e.g., timestamp or batch number) to each batch to differentiate new and existing data.

2. **Incremental Processing**:
   Process new batches in isolation to maintain the integrity of previous transactions. Use composite primary keys to ensure uniqueness.

3. **Data Deduplication**:
   Identify and eliminate duplicates by comparing incoming transactions against existing ones using key attributes.

4. **Error Handling**:
   Log conflicts or ambiguous data separately to ensure smooth processing without affecting prior batches.

5. **Archiving**:
   Archive older transactions in a separate layer for long-term storage and auditing purposes.

Ensuring transaction integrity, simplifies data reconciliation, and minimizes conflicts between batches.