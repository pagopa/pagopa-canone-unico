# Prerequisites
- [azure-data-tables](https://docs.microsoft.com/en-us/python/api/overview/azure/data-tables-readme?view=azure-python)

`pip3 install -r requirements.txt`

# Scripts
## fix_ecconfig.py
The script iterates on `ecconfig` table items and removes whitespaces from `Iban` column 

### How-to
#### DEV
python3 fix_ecconfig.py --env=dev --account-key=<azure-access-key>

#### UAT

python3 fix_ecconfig.py --env=uat --account-key=<azure-access-key>

#### PROD

python3 fix_ecconfig.py --env=prod --account-key=<azure-access-key>

#### LOCAL

python3 fix_ecconfig.py --env=local

## generateEcConfigTable.py

### How to Use

```
python3 buildEcConfigTable.py --ec ./inputData/export_enti.csv \
--iban ./inputData/export_iban.csv \
--ipa ./inputData/export_ipa.csv \
--istat ./inputData/Codici-statistici-e-denominazioni-al-17_01_2023.csv \
--env d \
--key xxx
```
## update_cup_iban.py
Iterates over `ecConfigTable` and for each Creditor Institution makes a call to ApiConfig iban enhanced api to retrieve
the `CUP` iban or the last inserted one.
### How to Use

```
python3 update_cup_iban.py \ 
--subkey <APIM SUBKEY TO CALL APICONFIG> \
--apim-url <APIM BASE URL ES. "https://api.platform.pagopa.it/apiconfig/auth/api/v1/"> \
--tables-primary-storage-account-key <TABLE STORAGE PRIMARY KEY> \
--tables-storage-endpoint-suffix <TABLE STORAGE ENDPOINT SUFFIX ES. "core.windows.net"> \
--tables-storage-account-name <TABLE STORAGE ACCOUNT NAME> \
```