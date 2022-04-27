'''
This script fixes wrong IBAN
'''

import argparse
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureNamedKeyCredential

parser = argparse.ArgumentParser(description='Tool to fix wrong IBANs stored in Azure table storage', prog='fix_ecconfig.py')

parser.add_argument('--account-key', metavar='ACCOUNT_KEY', type=str, nargs='?',
                    help='Azure account name (default: local connection string)')

parser.add_argument('--table-name', metavar='TABLE_NAME', type=str, nargs='?',
                    help='Azure table name (default: ecconfig)')

parser.add_argument('--env', metavar='env', type=str, nargs='?',
                    help='Azure subscription (default: local')

args = parser.parse_args()

env = args.env or "local"
account_key = args.account_key or "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

if env == "local":
    account_name = "devstoreaccount1"
    endpoint = "http://127.0.0.1:10002/{}".format(account_name)
    table_name = args.table_name or "ecconfig"
else:
    account_name = "pagopa{}canoneunicosa".format(env[0])
    table_name = args.table_name or "pagopa{}canoneunicosaecconfigtable".format(env[0])
    endpoint = "https://{}.table.core.windows.net/".format(account_name)

print([env, account_name, endpoint, table_name], sep="|")
credential = AzureNamedKeyCredential(account_name, account_key)

with TableServiceClient(endpoint=endpoint, credential=credential) as service:
    table = service.get_table_client(table_name=table_name)
    for entity in table.list_entities():
        if len(entity["Iban"]) > 27:
            print(entity)
            entity["Iban"] = entity["Iban"].replace(" ", "")
            table.update_entity(entity=entity)
