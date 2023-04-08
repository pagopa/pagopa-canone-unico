import argparse
import csv
import re
import os
import operator
import time
from datetime import datetime
from pathlib import Path


from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureNamedKeyCredential

# ############# Parse csv content into Dictionary ##################
def load_table(filename, separator, key, val):
    print("load_table|loading file [" + filename + "]")
    # open the file in read mode
    filename = open(filename, 'r')
    file = csv.reader(filename, delimiter=separator)
    next(file, None)

    # creating empty dict
    tab = {}
 
    # iterating over each row and add values to dictionary
    for col in file:
        tab[col[key]] = col[val]

    print("load_table|file loaded")
    return tab

def format_date(dateToFromat, sep):
    if not len(dateToFromat) > 0:
        print(">>>>>>> empty iban date")
        return datetime(1900, 1, 1)
    tkDate = dateToFromat.split(sep)
    return datetime(int(tkDate[2]), int(tkDate[1]), int(tkDate[0]))

# #################### Compute CI iban export csv file #####################
# 1. Load the file
# 2. Map the iban(s) to CI fiscal code
# 3. Sort the iban on activation date
# 4. Filter for eventual CUP label or alternatively on last activation date
# ###########################################################################
def load_iban_table(filePath, cupLabel):
    print("load_iban_table|loading file [" + filePath + "]")
    filename = open(filePath, 'r')
    file = csv.reader(filename, delimiter=';')
    next(file, None)

    # creating empty dictionary
    tab = {}

    # iterating over each row
    print("load_iban_table|Cycling over file rows")
    for row in file:
        # check if fiscalcode entry already exists
        if not row[1] in tab: # the dicationary does not exist
            # only active iban are taken into account
            #if row[5] == 'ATTIVO':
            if 'ATTIVO' in row[5]:
                tab[row[1]] = [{'fiscalCode': row[1], 'iban': row[3], 'state': row[5], 'actDate': row[6], 'desc': row[7]}]
        else:
            #if row[5] == 'ATTIVO':
            if 'ATTIVO' in row[5]:
                el = tab[row[1]]
                el.append({'fiscalCode': row[1], 'iban': row[3], 'state': row[5], 'actDate': row[6], 'desc': row[7]})
    
    # sort iban list belonging to each CI
    print("load_iban_table|sort dict by iban activation date")
    markedIbanCount = 0
    for fiscalCode in tab:
        ibanList = tab[fiscalCode]
        # sort the iban list by date, first reverse = false  because in case of more than one checked
        # iban we wat to keep the last activated one
        ibanList = sorted(ibanList, key=lambda d: format_date(d['actDate'], '/'), reverse=True)

        # iterate over the iban list looking for the CUP flag
        rightIban = {}
        for row in ibanList:
            if re.search(cupLabel, row['desc'], re.IGNORECASE):
                # found iban marked from EC
                rightIban = row
                markedIbanCount += 1

        # if checked iban found
        if bool(rightIban):
            # put into the dictionary
            tab[fiscalCode] = rightIban
        else:
            # no choise from CI, check if almost one iban is present
            if len(ibanList) > 0:
                # keep the last activated iban so we need to sort reverse the list
                tab[fiscalCode] = ibanList[0]
            else:
                # no active iban were present for EC set dict to empty
                tab[fiscalCode] = {}

    print("load_iban_table|found " + str(markedIbanCount) + " IBAN marked as CUP")
    print("load_iban_table|iban file elaborated")
    return tab

# ############################################################################
# Collect data from ibanTable, statsTable, ipaTable and build the final array 
# of dictionaries to import to Azure Table
# ############################################################################
def build_config_table(filePath, ibanTable, statsTable, ipaTable):
    print("build_config_table|reading csv file")
    filename = open(filePath, 'r')
    file = csv.reader(filename, delimiter=',')
    next(file, None)

    table = []

    print("build_config_table|cycling over file rows")
    noIbanCount = 0
    noIstatCodeCount = 0
    for line in file:
        #reset values
        iban = ""
        paIdCatasto = ""
        paIdIstat = ""
        
        # find CI in iban table
        if line[2] in ibanTable:
            iban = ibanTable[line[2]]['iban']
        else:
            noIbanCount += 1
            #print("iban not found for EC: " + line[2])
            print("build_config_table|iban not found for EC: " + line[2])       

        # correct the idCatasto, it's necessary to erase _C prefix in order to be compliant with stats table    
        if line[1].startswith("c_") | line[1].startswith("C_"):
            paIdCatasto = line[1][2:].upper()
        else:
            paIdCatasto = line[1]

        # match id catasto export_ec -> Codici-statistici-e-denominazioni-al-17_01_2023
        if paIdCatasto in statsTable:        
            paIdIstat = statsTable[paIdCatasto]
        else: 
            paIdIstat = "N/A"
            noIstatCodeCount += 1
            #print("paIdIstat not found for EC: " + line[2])

       # setting up the dictionary record 
        tableRecord = { 'PartitionKey': 'org', 
            'RowKey' : line[2], 
            'Timestamp' : datetime.now(), 
            'CompanyName' : line[0], 
            'Iban' : iban, 
            'PaIdCatasto' : paIdCatasto, 
            'PaIdCbill' : line[27], 
            'PaIdIstat' : paIdIstat, 
            'PaPecEmail' : ipaTable[line[2]], 
            'PaReferentEmail' : line[8], 
            'PaReferentName' : line[6] + " " + line[5]}

        # add line to table 
        table.append(tableRecord)

    print("build_config_table|CI without iban match: " + str(noIbanCount))
    print("build_config_table|CI without istat code match: " + str(noIstatCodeCount))
    print("build_config_table|Table created")

    return table

# ############ Print table content to csv file ################# 
def write_csv_table(filePath, ecTable):
    
    headers = ['PartitionKey', 
                'RowKey', 
                'Timestamp', 
                'CompanyName', 
                'Iban', 
                'PaIdCatasto', 
                'PaIdCbill', 
                'PaIdIstat', 
                'PaPecEmail', 
                'PaReferentEmail', 
                'PaReferentName']
    
    print("write_csv_table|opening output file")
    with open(filePath, 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        # write the header
        print("write_csv_table|writing headers")
        writer.writeheader()

        # write multiple rows
        print("write_csv_table|writing content")
        writer.writerows(ecTable)
        print("write_csv_table|csv file completed")

# ######### Load Azure config table by env (to be optimized) #######################
def load_config_table(env, ecTable):
    print("load_config_table|loading Azure table")
    if env == "local":
        print("load_config_table|env local")
        account_name = "devstoreaccount1"
        endpoint = "http://127.0.0.1:10002/{}".format(account_name)
        table_name = "pagopapcanoneunicosaecconfigtable"
        accountKey = args.key
    else:
        print("load_config_table|env " + env)
        account_name = "pagopa{}canoneunicosa".format(env)
        table_name = "pagopa{}canoneunicosaecconfigtable".format(env)
        endpoint = "https://{}.table.core.windows.net/".format(account_name)
        accountKey = args.key
        

    print([env, account_name, endpoint, table_name], sep="|")
    credential = AzureNamedKeyCredential(account_name, accountKey)

    recordCount = 0
    with TableServiceClient(endpoint=endpoint, credential=credential) as service:
        table = service.get_table_client(table_name=table_name)
        for myEntity in ecTable:
            table.create_entity(entity=myEntity)
            recordCount += 1
    
    print("load_config_table|record inserted: " + str(recordCount))


parser = argparse.ArgumentParser()

parser.add_argument('--ec', type=Path, required=True, help='Path to the \'enti\' CSV file.')
parser.add_argument('--iban', type=Path, required=True, help='Path to the \'iban\' CSV file.')
parser.add_argument('--ipa', type=Path, required=True, help='Path to the \'ipa\' CSV file.')
parser.add_argument('--istat', type=Path, required=True, help='Path to the \'istat\' CSV file.')
parser.add_argument('--env', type=str, required=True, choices=['local', 'd', 'u', 'p'],
                    help='Environment to upload the CSV')
parser.add_argument('--key', type=str, required=True, help='To to authenticate on Azure storage account')

args = parser.parse_args()


# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

# load iban table
ibanTable = load_iban_table(f'{dirname}{args.iban}', "canone unico")
# load ipa table
ipaTable = load_table(f'{dirname}{args.ipa}', ';', 3, 8)
# load statistics table
statsTable = load_table(f'{dirname}{args.istat}', ';', 19, 4)
# build config table in memory 
ecTable = build_config_table(f'{dirname}{args.ec}', ibanTable, statsTable, ipaTable)

# write config table to csv
write_csv_table(dirname + "/outputData/ecConfigTable.csv", ecTable)

# write config table to Azure table
load_config_table(args.env, ecTable)
