# MLOps Retail Data Platform — Sprint 1 Plan

## Goal
Set up the initial project structure for a collaborative MLOps and analytics project.

## Data Source
Large CSV files are stored outside GitHub in Azure Blob Storage.

Raw data lives in the Azure Storage Account `ozzymldata2410` under the
`mlops-data` container. Local raw CSV files belong in `data/raw`, which is
ignored by Git so large data assets are not committed to GitHub. The
`src/download_raw_data.py` utility is the bridge between Azure Blob Storage and
local development: it will use environment variables for Azure connection
details and prepare/download the expected raw files into the local raw data
folder.

GitHub Actions now checks the Python project automatically on pushes and pull
requests. This CI workflow helps the team catch syntax errors, unsafe imports,
and accidentally committed raw CSV files before changes are merged. Raw data
should remain outside GitHub and continue to live in Azure Blob Storage.

## Sprint 1 Tasks
- Create GitHub repository structure
- Define data folder strategy
- Prepare Python dependency file
- Prepare CI/CD folder
- Document project direction

## Team Roles
- ML Engineer: model development, training pipeline, evaluation
- Data Analyst / Analytics Engineer: data modelling, metrics, dashboard logic
