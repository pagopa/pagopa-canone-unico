import argparse
import csv
from pathlib import Path
import os

# ### Check if the config table includes the configuration specified in the partner file ### 
def check_partner_file(partnerFile, tableDataFile, keyP, keyT):

    # load the ecConfigTable
    print("check_partner_file_by_fiscal_code|loading file [" + tableDataFile + "]")
    filename = open(tableDataFile, 'r')
    file = csv.reader(filename, delimiter=',')
    
    # skip the headers line
    next(file, None)

    # creating empty dictionary
    tableConfig = {}

    # load the table config key (fiscalcode or idcatasto) the value is not necessary
    for row in file:
        tableConfig[row[keyT]] = ""

    print("check_partner_file_by_fiscal_code|file [" + tableDataFile + "] loaded")

    # load the partner file
    print("check_partner_file_by_fiscal_code|cycling over file [" + partnerFile + "]")
    filename = open(partnerFile, 'r')
    file = csv.reader(filename, delimiter=';')

    # skip the headers line
    next(file, None)

    nfCounter = 0
    for row in file:
        # check if the code (fiscalcode or idcatasto) specified by the partner is in ecconfigTable
        if (not row[keyP] in tableConfig):
            nfCounter += 1
            print("check_partner_file_by_fiscal_code|config not found for code " + row[keyP])

    print("check_partner_file_by_fiscal_code|file [" + partnerFile + "] elaborated")

    print("check_partner_file_by_fiscal_code|code not found: [" + str(nfCounter) + "]")

    if nfCounter == 0:
        print("check_partner_file_by_fiscal_code|partner file is compliant with ecConfigTable")
    else:
        print("check_partner_file_by_fiscal_code|partner file is not compliant, please check the configuration")




# read arguments
parser = argparse.ArgumentParser()
parser.add_argument('--partnerFile', type=Path, required=True, help='Path to the partner CSV file to load')
parser.add_argument('--tableFile', type=Path, required=True, help='Path to the ecConfigTable CSV file')
parser.add_argument('--scanBy', type=Path, required=True, default='fiscalCode', help='Scan by fiscalCode or idCatasto')
args = parser.parse_args()

# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

# check if it's necessary to scan partner file for fiscal code or is catasto 
if str(args.scanBy) == 'idCatasto':
    export_ec = check_partner_file(f'{dirname}{args.partnerFile}', f'{dirname}{args.tableFile}', 2, 5)
else:
    export_ec = check_partner_file(f'{dirname}{args.partnerFile}', f'{dirname}{args.tableFile}', 3, 1)
