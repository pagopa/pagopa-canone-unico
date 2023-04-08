import argparse
import csv
from pathlib import Path
import os

def check_partner_file_by_fiscal_code(partnerFile, tableDataFile):
    print("check_partner_file_by_fiscal_code|loading file [" + tableDataFile + "]")
    filename = open(tableDataFile, 'r')
    file = csv.reader(filename, delimiter=',')
    
    # skip the headers line
    next(file, None)

    # creating empty dictionary
    tableConfig = {}

    for row in file:
        tableConfig[row[1]] = ""

    print("check_partner_file_by_fiscal_code|file [" + tableDataFile + "] loaded")

    print("check_partner_file_by_fiscal_code|cycling over file [" + partnerFile + "]")
    filename = open(partnerFile, 'r')
    file = csv.reader(filename, delimiter=';')
    # skip the headers line
    next(file, None)
    for row in file:
        if (not row[3] in tableConfig):
            print("check_partner_file_by_fiscal_code|config not found for Fiscal Code " + row[3])

    print("check_partner_file_by_fiscal_code|file [" + partnerFile + "] elaborated")

parser = argparse.ArgumentParser()

parser.add_argument('--partnerFile', type=Path, required=True, help='Path to the \'export_ec\' CSV file.')
parser.add_argument('--tableFile', type=Path, required=True, help='Path to the \'lista_comuni\' CSV file.')
args = parser.parse_args()

# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

export_ec = check_partner_file_by_fiscal_code(f'{dirname}{args.partnerFile}', f'{dirname}{args.tableFile}')