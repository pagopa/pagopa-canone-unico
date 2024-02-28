import argparse
import csv
import re
import os
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

# ######## set the date format to "%Y-%m-%d" for sorted algorithm ########
def format_date(dateToFromat, sep):
    # sometimes the iban is not specified, in order to properly sort the date
    # it's setted to "1900-01-01"
    if not len(dateToFromat) > 0:
        print(">>>>>>> empty iban date")
        return datetime(1900, 1, 1)
    
    tkDate = dateToFromat.split(sep)
    return datetime(int(tkDate[2]), int(tkDate[1]), int(tkDate[0]))

# ## load idIstat, description and idCatasto from istat table ##############
def load_istat_table(filePath):
    print("load_istat_table|loading file [" + filePath + "]")
    filename = open(filePath, 'r')
    file = csv.reader(filename, delimiter=';')
    next(file, None)

    # creating empty dictionary
    tab = {}

    # iterating over each row
    print("load_istat_table|Cycling over file rows")
    for row in file:
        tab[row[19]] = [{'companyName': row[5], 'idIstat': row[4]}]

    print("load_istat_table|iban file elaborated")
    return tab

# #################### Compute CUP iban export from PDND #####################
# 1. Load the file
# 2. Map the iban(s) to CI fiscal code
# 3. Sort the iban on activation date
# ###########################################################################
def load_iban_table_cup(filePath):
    print("load_iban_table_cup|loading file [" + filePath + "]")
    filename = open(filePath, 'r')
    file = csv.reader(filename, delimiter=';')
    next(file, None)

    # creating empty dictionary
    tab = {}

    # iterating over each row
    print("load_iban_table_cup|Cycling over file rows")
    for row in file:
        # check if fiscalcode entry already exists
        if not row[1] in tab: # the dicationary does not exist
            tab[row[1]] = [{'fiscalCode': row[1], 'iban': row[2], 'actDate': row[6]}]
        else:
            el = tab[row[1]]
            el.append({'fiscalCode': row[1], 'iban': row[2], 'actDate': row[6]})
    
    # sort iban list belonging to each CI
    print("load_iban_table_cup|sort dict by iban activation date")
    markedIbanCount = 0
    for fiscalCode in tab:
        ibanList = tab[fiscalCode]
        # sort the iban list by date, first reverse = false  because in case of more than one checked
        # iban we wat to keep the last activated one
        ibanList = sorted(ibanList, key=lambda d: d['actDate'], reverse=True)

        if len(ibanList) > 0:
            # keep the last activated iban so we need to sort reverse the list
            tab[fiscalCode] = ibanList[0]
        else:
            # no active iban were present for EC set dict to empty
            tab[fiscalCode] = {}

    print("load_iban_table_cup|iban file elaborated")
    return tab

# #################### Compute CI iban export csv file #####################
# 1. Load the file
# 2. Map the iban(s) to CI fiscal code
# 3. Sort the iban on activation date
# 4. Filter for eventual CUP label or alternatively on last activation date
# ###########################################################################
def load_iban_table(filePath, cupIbanTable):
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

        # check if CUP iban exixts
        if fiscalCode in cupIbanTable:
            # put into the dictionary
            tab[fiscalCode] = cupIbanTable[fiscalCode]
            markedIbanCount += 1
        else:
            # no cup choise, check if almost one iban is present
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
    
    newIstatTable = []
    keyList = list(statsTable.keys())
    for key in keyList:
        newIstatTable.append({"idCatasto": key, 
            "companyName": statsTable[key][0]['companyName'],
            "idIstat": statsTable[key][0]['idIstat']})
            
    print("build_config_table|cycling over file rows")
    noIbanCount = 0
    noIstatCodeCount = 0
    #recoveredIstatCodeCount = 0
    #recoveredWrongIstatTable = 0

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
            paIdIstat = statsTable[paIdCatasto][0]['idIstat']
        else: 
            paIdIstat = "N/A"
            noIstatCodeCount += 1

            # try to recover istat code
            #for row in newIstatTable:
            #    cName = row['companyName']
            #    if cName in line[0]:
            #        paIdIstat = row['idIstat']
            #        recoveredIstatCodeCount += 1

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

    #for line in table:
    #    cName = line['CompanyName']
    #    for el in wrongIstatTable:
    #        wName = el
    #        if wName.lower() in cName.lower():
    #            for row in newIstatTable:
    #                if row['companyName'].lower() in cName.lower():
    #                    line['PaIdIstat'] = row['idIstat']
    #                    line['PaIdCatasto'] = row['idCatasto']
        

    print("build_config_table|CI without iban match: " + str(noIbanCount))
    print("build_config_table|CI without istat code match: " + str(noIstatCodeCount))
    #print("build_config_table|istat code recovered: " + str(recoveredIstatCodeCount))
    #print("build_config_table|wrong istat code recovered: " + str(recoveredWrongIstatTable))
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

# ######## Add missing CF to table (CI not configured on pagoPA platform) #########################
def load_missing_ci(ecTable):

    ecTable.append({"PartitionKey": "org", "RowKey": "84000730659", "Timestamp": "2022-04-14T20:04:32.7313004Z", "CompanyName": "Comune di Ascea", "Iban": "", "PaIdCatasto": "A460", "PaIdCbill": "n.a", "PaIdIstat": "065009", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "00747990166", "Timestamp": "2022-04-14T20:04:32.8302442Z", "CompanyName": "Comune di Brumano", "Iban": "", "PaIdCatasto": "B217", "PaIdCbill": "n.a", "PaIdIstat": "016041", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "80001270943", "Timestamp": "2022-04-14T20:04:33.042124Z", "CompanyName": "Comune di Conca Casale", "Iban": "", "PaIdCatasto": "C941", "PaIdCbill": "n.a", "PaIdIstat": "094018", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "82001730694", "Timestamp": "2022-04-14T20:04:33.1460649Z", "CompanyName": "Comune di Crecchio", "Iban": "", "PaIdCatasto": "D137", "PaIdCbill": "n.a", "PaIdIstat": "069027", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "00371570797", "Timestamp": "2022-04-14T20:04:33.2390123Z", "CompanyName": "Comune di Filogaso", "Iban": "", "PaIdCatasto": "D596", "PaIdCbill": "n.a", "PaIdIstat": "102013", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "81002200160", "Timestamp": "2022-04-14T20:04:33.3409549Z", "CompanyName": "Comune di Monasterolo del Castello", "Iban": "", "PaIdCatasto": "F328", "PaIdCbill": "n.a", "PaIdIstat": "016137", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "81000890780", "Timestamp": "2022-04-14T20:04:33.5668259Z", "CompanyName": "Comune di Nocara", "Iban": "", "PaIdCatasto": "F907", "PaIdCbill": "n.a", "PaIdIstat": "078086", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "00125240598", "Timestamp": "2022-04-14T20:04:33.6807614Z", "CompanyName": "Comune di Norma", "Iban": "", "PaIdCatasto": "F937", "PaIdCbill": "n.a", "PaIdIstat": "059016", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "90002510619", "Timestamp": "2022-04-14T20:04:33.7817038Z", "CompanyName": "Comune di Orta di Atella", "Iban": "", "PaIdCatasto": "G130", "PaIdCbill": "n.a", "PaIdIstat": "061053", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "80009710643", "Timestamp": "2022-04-14T20:04:33.8786499Z", "CompanyName": "Comune di Roccabascerana", "Iban": "", "PaIdCatasto": "H382", "PaIdCbill": "n.a", "PaIdIstat": "064078", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})
    ecTable.append({"PartitionKey": "org", "RowKey": "81000610618", "Timestamp": "2022-04-14T20:04:33.9935851Z", "CompanyName": "Comune di Villa Literno", "Iban": "", "PaIdCatasto": "L844", "PaIdCbill": "n.a", "PaIdIstat": "061099", "PaPecEmail": "n.a", "PaReferentEmail": "n.a", "PaReferentName": "n.a"})  


parser = argparse.ArgumentParser()

parser.add_argument('--ec', type=Path, required=True, help='Path to the \'enti\' CSV file.')
parser.add_argument('--iban', type=Path, required=True, help='Path to the \'iban\' CSV file.')
parser.add_argument('--ibanCUP', type=Path, required=True, help='Path to the CSV file with iban marked as CUP.')
parser.add_argument('--ipa', type=Path, required=True, help='Path to the \'ipa\' CSV file.')
parser.add_argument('--istat', type=Path, required=True, help='Path to the \'istat\' CSV file.')
parser.add_argument('--env', type=str, required=True, choices=['local', 'd', 'u', 'p'],
                    help='Environment to upload the CSV')
parser.add_argument('--key', type=str, required=True, help='To to authenticate on Azure storage account')

args = parser.parse_args()


# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

ibanCupTable = load_iban_table_cup(f'{dirname}{args.ibanCUP}')

# load iban table
ibanTable = load_iban_table(f'{dirname}{args.iban}', ibanCupTable)
# load ipa table
ipaTable = load_table(f'{dirname}{args.ipa}', ';', 3, 8)

# load statistics table
#statsTable = load_table(f'{dirname}{args.istat}', ';', 19, 4)
statsTable = load_istat_table(f'{dirname}{args.istat}')

# build config table in memory 
ecTable = build_config_table(f'{dirname}{args.ec}', 
                             ibanTable, 
                             statsTable, 
                             ipaTable)

# Add missing CF to table (CI not configured on pagoPA platform)
load_missing_ci(ecTable)

# write config table to csv
write_csv_table(dirname + "/outputData/ecConfigTable.csv", ecTable)

# write config table to Azure table
load_config_table(args.env, ecTable)
