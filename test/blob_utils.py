import argparse
import csv
import os
import random
import sys
import time
from enum import Enum

random.seed(time.time())


class Action(Enum):
    List = "list"
    Upload = "upload"
    Download = "download"


parser = argparse.ArgumentParser(description='Tool to upload files in Azure blob storage', prog='blob_utils.py')
parser.add_argument('action', metavar='ACTION', type=Action, choices=list(Action),
                    help='action to perform (list/upload/download)')
parser.add_argument('--file', metavar='FILE', type=str, required="upload" in sys.argv or "download" in sys.argv,
                    help='file to upload')
parser.add_argument('--path', metavar='PATH', type=str, nargs='?', help='directory of intput/output (default: ./)')
parser.add_argument('--rows', metavar='ROWS', type=int, nargs='?', help='numbers of CSV rows to generate (default: 1)')
parser.add_argument('--pa-fiscalcode', metavar='PA_FISCALCODE', type=str, nargs='?',
                    help='PA fiscalcode to upload (size 11)')
parser.add_argument('--account-name', metavar='ACCOUNT_NAME', type=str, nargs='?',
                    help='Azure account name (default: pagopadcanoneunicosa)')
parser.add_argument('--container-name', metavar='CONTAINER_NAME', type=str, nargs='?',
                    help='Azure container name (default: pagopadcanoneunicosaincsvcontainer)')
parser.add_argument('--invalid_csv', metavar='INVALID_CSV', type=bool, nargs='?',
                    help='Invalid CSV format file (default: False)')
parser.add_argument('--invalid_csv_header', metavar='INVALID_CSV_HEADER', type=bool, nargs='?',
                    help='Invalid CSV header file (default: False)')
parser.add_argument('--not_mutually_id', metavar='NOT_MUTUALLY_ID', type=bool, nargs='?',
                    help='Invalid CSV header file (default: False)')
args = parser.parse_args()

action = args.action
file = args.file
path = args.path or "."
rows = args.rows
pa_fiscalcode = args.pa_fiscalcode or "11111111111"
account_name = args.account_name or "pagopadcanoneunicosa"
container_name = args.container_name or "pagopadcanoneunicosaincsvcontainer"
invalid_csv = args.invalid_csv or False
invalid_csv_header = args.invalid_csv_header or False
not_mutually_id = args.not_mutually_id or False

if action == Action.Upload:
    os.system(f'mkdir -p {path}')
    if rows:
        with open(path + "/" + file, 'w', encoding='UTF8') as f:
            writer = csv.writer(f, delimiter=';')
            
            if not(invalid_csv_header):
                header = ["id", "pa_id_istat", "pa_id_catasto", "pa_id_fiscal_code", "pa_id_cbill", "pa_pec_email",
                        "pa_referent_email",
                        "pa_referent_name", "amount", "debtor_id_fiscal_code", "debtor_name", "debtor_email",
                        "payment_notice_number",
                        "note"]
            else:
                header = ["a", "b", "c", "d", "e", "pa_pec_email",
                        "pa_referent_email",
                        "pa_referent_name", "amount", "debtor_id_fiscal_code", "debtor_name", "debtor_email",
                        "payment_notice_number",
                        "note"]
            # write the header
            writer.writerow(header)
            if not(invalid_csv):
                for i in range(1, rows + 1):
                    data = [i, "", "", pa_fiscalcode, "", "fake@email.com", "pa_referent_email", "pa_referent_name",
                            random.randint(1, 100000), str(random.randint(0, 99999999999)).zfill(11),
                            "Lorem ipsum", "lorem@pec.loremit", "", ""]
                    if not_mutually_id:
                        data[1] = data[3]
                        data[2] = data[3]
                    # write the data
                    writer.writerow(data)
            else: # invalid data case: missing pa_id_istat, pa_id_catasto
                for i in range(1, rows + 1):
                    data = [i, pa_fiscalcode, "", "fake@email.com", "pa_referent_email", "pa_referent_name",
                            random.randint(1, 100000), str(random.randint(0, 99999999999)).zfill(11),
                            "Lorem ipsum", "lorem@pec.loremit", "", ""]
                    # write the data
                    writer.writerow(data)
    blob = "input/" + file
    os.system(
        f'az storage blob upload --account-name {account_name} --auth-mode key -c {container_name} -f {path}/{file} -n {blob}')

elif action == Action.List:
    os.system(
        f'az storage blob list --account-name {account_name} --auth-mode key -c {container_name} --output table')

elif action == Action.Download:
    os.system(f'mkdir -p {path}')
    os.system(
        f'az storage blob download --account-name {account_name} --auth-mode key -c {container_name} -f {path}/{file} -n {file}')
