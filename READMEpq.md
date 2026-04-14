# Finance ETL Pipeline

Finance Advertising and Promotion (A&P) ETL pipeline. This pipeline enriches data extracted from *NEO* to create a dataset for *Budget Owners* and *Business Finance*. Budget Owners are able to build budgets, manage spending and validate funds. Meanwhile, Business Finance can monitor expenditure, actualize and reconcile spend and view spend details by attributes such as Profit and Loss (P&L) lines and more.  

## Overview

There are three fiscal scenarios that this pipeline ingests and transforms: *actual*, *committed*, and *forecast*.

- **actual**: expenses that have already posted in NEO
- **committed**: proposed funding towards a project inputted in NEO
- **forecast**: predicted costs that are uploaded by budget owners
  - These are further divided into *pre-budget*, *budget*, *trend 3*, *trend 5*, *trend 9*, *live estimate*.

**Supplementary attributes**

Each of the three spending areas are supplemented with additional attributes stemming from one of the following entities:

- Company Codes: country identifers
- Cost Centers (CC):
- Cost Center Details: additional dataset that further inspects each cost center expense
- Transactional Chart of Accounts (TCOA): the parent-child definitions of the P&L lines
- FAGL_ZF: bridges GL accounts and Compass codes
- Financial Statement (FS) items: the Compass code and its name
- General Ledger (GL) accounts:
- Profit Centers (PC): bridges profit centers and signatures
- Signatures (Sig): defines the name of the signature and division
- Standard Hierarchy Node: serves as a bridge between CC and Compass codes
- WBS Elements (WBS)

These files are also extracted from NEO.

**Additional data**

Additional data that is not extracted from NEO include:

- Commited Spend for WBS
- Committed Spend for CC
- WBS Budgets

## ETL Process

### Extract 
- Read and combine data from *-US-FinanceAP* SharePoint directories.
  - `SharePoint.Contents("https://loreal.sharepoint.com/sites/-US-FinanceAP/", [ApiVersion = 15])`

### Transform
- Normalize source columns using `NormaizeColumnNames` function.
- Transform supplementary data.
- Enrich records with supplementary data (see Overview).
- Combine all data to create one fact table, `fact_Figures`.

### Load
- Create the star schema using `fact_Figures` and the define supplementary data as the dimensions.

### Directory structure

```text
FinanceAP  /
└── AP Tracker/
    ├── Inputs/
    │   ├── 002A/
    │   │   ├── Archive/
    │   │   │   └── *.csv
    │   │   └── *.csv
    │   ├── 003A/
    │   │   ├── Archive/
    │   │   │   └── *.csv
    │   │   └── *.csv
    │   └── {Company Code}/
    │       ├── Archive/
    │       │   └── *.csv
    │       └── *.csv
    ├── Forecast/
    │   ├── CPD/
    │   │   ├── Budget/
    │   │   │   └── *.csv
    │   │   ├── Live Estimate/
    │   │   │   └── *.csv
    │   │   ├── Pre-Budget/
    │   │   │   └── *.csv
    │   │   └── Trend/
    │   │       └── *.csv
    │   ├── LDB/
    │   │   ├── Budget
    │   │   ├── Live Estimate
    │   │   ├── Pre-Budget
    │   │   └── Trend
    │   ├── LLD/
    │   │   ├── Budget
    │   │   ├── Live Estimate
    │   │   ├── Pre-Budget
    │   │   └── Trend
    │   └── PPD/
    │       ├── Budget
    │       ├── Live Estimate
    │       ├── Pre-Budget
    │       └── Trend
    └── Metadata/
        └── *.csv
```
## General schema


| Column Name                     | Data Type        |
| ------------------------------- | ---------------- |
| Company Code                    | VARCHAR          |
| Structure                       | VARCHAR          |
| Semantic Tag                    | VARCHAR          |
| Signature                       | VARCHAR          |
| Signature Code                  | VARCHAR          |
| Signature2                      | VARCHAR          |
| Accounting doc type             | VARCHAR          |
| Fiscal Year                     | BIGINT           |
| Fiscal Period                   | BIGINT           |
| Ledger                          | VARCHAR          |
| Profit Center                   | VARCHAR          |
| Profit Center Name              | VARCHAR          |
| Distribution Channel            | BIGINT           |
| Material                        | VARCHAR          |
| G/L Account                     | BIGINT           |
| G/L Account Name                | VARCHAR          |
| G/L Account Type                | VARCHAR          |
| Journal Entry Type              | VARCHAR          |
| JE Type Name                    | VARCHAR          |
| Journal Entry Item Text         | TEXT             |
| Amount in Company Code Currency | DOUBLE PRECISION |
| Purchasing Document             | VARCHAR          |
| Purchasing Doc. Item            | BIGINT           |
| Cost Center                     | VARCHAR          |
| Cost Center Name                | VARCHAR          |
| Partner Cost Center             | VARCHAR          |
| Project                         | VARCHAR          |
| Project Name                    | VARCHAR          |
| WBS Element External ID         | VARCHAR          |
| WBS Element                     | VARCHAR          |
| WBS Element Name                | VARCHAR          |
| Product                         | VARCHAR          |
| Reference Document Category     | VARCHAR          |
| Debit Date                      | TIMESTAMP        |
| Object Type                     | VARCHAR          |
| Project definition              | VARCHAR          |
| Object                          | VARCHAR          |
| Cost element                    | BIGINT           |
| Cost element desc.              | VARCHAR          |
| Value in Obj. Crcy              | DOUBLE PRECISION |
| Val.in rep.cur.                 | DOUBLE PRECISION |
| Total Quantity                  | DOUBLE PRECISION |
| Quantity/Plan                   | DOUBLE PRECISION |
| Object Currency                 | VARCHAR(3)       |
| Unit of Measure                 | VARCHAR          |
| Value TranCurr                  | DOUBLE PRECISION |
| Val/COArea Crcy                 | DOUBLE PRECISION |
| User Name                       | VARCHAR          |
| Supplier                        | VARCHAR          |
| Ref. document number            | VARCHAR          |
| Reference Item                  | BIGINT           |
| Reference Doc . Type            | VARCHAR          |
| Reference date                  | TIMESTAMP        |
| Name                            | VARCHAR          |
| Period                          | BIGINT           |
| Document Date                   | TIMESTAMP        |
| Business Transaction            | VARCHAR          |
| CO Object Name                  | VARCHAR          |

## Data Freshness

- **Actual** data will be provided at least weekly per company code.
- **Commmited** data is provided at the beginning of the year per company code.
- **Forecast** data is provided at various frequencies
  - Three times a day during the first and last week of each month.
  - Once daily during the second and third week
  - Ad-hoc during times of high business activity.