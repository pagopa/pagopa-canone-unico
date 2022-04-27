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
