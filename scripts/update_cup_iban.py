import argparse
import requests
from requests.exceptions import HTTPError
from azure.data.tables import TableClient
from azure.data.tables import UpdateMode


class UpdateIban(object):
    def __init__(self):
        print("[INFO][__init__] reading args parameters")
        parser = argparse.ArgumentParser()
        parser.add_argument('--subkey', required=True, help='subkey to invoke apim endpoint')
        parser.add_argument('--apim-url', required=True, help='apim URL')
        parser.add_argument('--tables-primary-storage-account-key', required=True, help='')
        parser.add_argument('--tables-storage-endpoint-suffix', required=True, help='')
        parser.add_argument('--tables-storage-account-name', required=True, help='')
        args = parser.parse_args()
        print("[INFO][__init__] parameters correctly oarsed")

        print("[INFO][__init__] getting parameters values")
        access_key = str(args.tables_primary_storage_account_key)
        endpoint_suffix = str(args.tables_storage_endpoint_suffix)
        account_name = str(args.tables_storage_account_name)
        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={access_key};EndpointSuffix={endpoint_suffix}"
        self.table_base = "pagopapcanoneunicosaecconfigtable"
        self.subkey = str(args.subkey)
        self.apim_url = str(args.apim_url)
        print("[INFO][__init__] parameters correctly retrieved")

    def update_entities(self):

        print("[INFO][update_entities] connecting to ecconfigtable")
        try:
            with TableClient.from_connection_string(self.connection_string, table_name=self.table_base) as table:

                print("[INFO][update_entities] iterating over ecconfigtable")
                entities = list(table.list_entities())
                for i, entity in enumerate(entities):
                    id_ci = entity['RowKey']
                    cup_iban = self.get_iban_by_ci(id_ci)
                    if cup_iban == entity['Iban']:
                        entity['Iban'] = cup_iban
                        insert_entity = table.upsert_entity(mode=UpdateMode.REPLACE, entity=entity)
                        print(f"[INFO][update_entities] updated entity: {insert_entity}")
                    else:
                        print(f"[INFO][update_entities] iban {entity['Iban']} is already updated with CUP iban {cup_iban}")

        except Exception as e:
            print(f"[ERROR][update_entities] HTTP error occurred: {e}")

    def get_iban_by_ci(self, id_ci):

        print(f"[INFO][get_iban_by_ci] getting CUP iban for {id_ci} EC")
        try:
            api_url = f"{self.apim_url}/creditorinstitutions/{id_ci}/ibans/enhanced?label=0201138TS"
            headers = {'Ocp-Apim-Subscription-Key': self.subkey}

            print(f"[INFO][get_iban_by_ci] calling api: {api_url}")
            response = requests.get(api_url, headers=headers)
            print(f"[INFO][get_iban_by_ci] response code: {response.status_code}")
            response.raise_for_status()
            json_response = response.json()
            iban_enhanced = json_response['ibans_enhanced']
            cup_iban = ""
            if len(iban_enhanced) > 0:
                cup_iban = iban_enhanced[0]['iban']
            else:
                print(f"[ERROR][get_iban_by_ci] no iban found associated to EC {id_ci}")

            print(f"[INFO][get_iban_by_ci] found cup iban {cup_iban} for EC {id_ci}")
            return cup_iban

        except HTTPError as http_err:
            print(f"[ERROR][get_iban_by_ci] HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"[INFO][get_iban_by_ci] generic error occurred: {err}")


if __name__ == "__main__":
    update_iban = UpdateIban()
    update_iban.update_entities()
