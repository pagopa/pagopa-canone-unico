# Prerequisites
- [azure-data-tables](https://docs.microsoft.com/en-us/python/api/overview/azure/data-tables-readme?view=azure-python)

`pip3 install -r requirements.txt`

Below the details to fill the `pagopapcanoneunicosaecconfigtable` table
## 1. Run generateEcConfigTable.py

### How to Use

```
python3 buildEcConfigTable.py --ec ./inputData/export_enti.csv \
--iban ./inputData/export_iban.csv \
--ipa ./inputData/export_ipa.csv \
--istat ./inputData/Codici-statistici-e-denominazioni-al-17_01_2023.csv \
--env d \
--key xxx
```

> **_WARNING:_**  Currentlly the script is based on the PdA export data that could be not fully udpated.
> In the future we need to get data from SelfCare as soon as new API will be provided.

## 2. Run update_cup_iban.py
Iterates over `pagopapcanoneunicosaecconfigtable` and for each Creditor Institution makes a call to ApiConfig iban enhanced api to retrieve
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
