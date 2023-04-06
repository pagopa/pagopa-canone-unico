### Prerequisites
- [azure-data-tables](https://docs.microsoft.com/en-us/python/api/overview/azure/data-tables-readme?view=azure-python)

`pip3 install -r requirements.txt`

### Scripts
#### fix_ecconfig.py
The script iterates on `ecconfig` table items and removes whitespaces from `Iban` column 

##### How-to
###### DEV
python3 fix_ecconfig.py --env=dev --account-key=<azure-access-key>

###### UAT

python3 fix_ecconfig.py --env=uat --account-key=<azure-access-key>

###### PROD

python3 fix_ecconfig.py --env=prod --account-key=<azure-access-key>

###### LOCAL

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
