# Swiss-Re-Insurance-Project  
Data Transformation Pipeline: Contracts and Claims to Transactions  

---

## Project Overview  

This project implements a **data transformation pipeline** using **Python** and **Apache Spark** to process contracts and claims data. The goal is to produce transactions adhering to a common internal data model, following the requirements outlined by the "Europe 3" source.  

The implementation is self-contained, covered by tests, and adheres to production-level coding standards. It processes input data, applies specified transformations, and saves the output in the desired format.  

---

## Features  

- Processes **contract** and **claim** datasets into a unified **transaction** dataset.  
- Implements business rules and mappings as specified:  
  - Converts `CLAIM_TYPE` to `TRANSACTION_TYPE`.  
  - Maps `CLAIM_ID` prefixes to `TRANSACTION_DIRECTION`.  
  - Generates unique IDs for transactions via a REST API.  
- Fully tested with automated unit tests for validation.  
- Supports additional batches of data without overwriting existing transactions.  

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
- **Pandas (`pandas>=2.2.1`)**: For additional data manipulation and processing.  
- **Python-Dateutil (`python-dateutil>=2.8.2`)**: To handle date transformations.  
- **Pytest (`pytest>=8.1.1`)**: For testing and validating the implementation.  

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
2. Test the implementation:
   ```bash
   pytest

### License
Do not share or distribute this code without prior authorization.