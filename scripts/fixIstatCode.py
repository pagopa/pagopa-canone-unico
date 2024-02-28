import argparse
import csv
import os
from datetime import datetime
from pathlib import Path
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import UpdateMode

# ######### Load Azure config table by env (to be optimized) #######################
def fix_istat_code(env, statsFilePath):
    print("fix_istat_code|loading Azure table")
    if env == "local":
        print("fix_istat_code|env local")
        account_name = "devstoreaccount1"
        endpoint = "http://127.0.0.1:10002/{}".format(account_name)
        table_name = "pagopapcanoneunicosaecconfigtable"
        accountKey = args.key
    else:
        print("fix_istat_code|env " + env)
        account_name = "pagopa{}canoneunicosa".format(env)
        table_name = "pagopa{}canoneunicosaecconfigtable".format(env)
        endpoint = "https://{}.table.core.windows.net/".format(account_name)
        accountKey = args.key
        

    print([env, account_name, endpoint, table_name], sep="|")
    credential = AzureNamedKeyCredential(account_name, accountKey)

    # load stats table
    statsTable = {}
    print("fix_istat_code|reading csv file")
    filename = open(statsFilePath, 'r')
    file = csv.reader(filename, delimiter=';')
    next(file, None)
    for line in file:
        statsTable[line[4]] = line


    # coede to fix
 #    istatCodeToFix = ["001099", "001140", "001308", "001313", "004119", "004123", "005058", 
 #                     "005122", "008004", "008011", "008021", "008051", "008056", "008061", 
 #                     "009015", "009020", "010058", "012056", "012091", "012144", "013120", 
 #                     "013139", "013157", "013238", "013250", "014013", "015172", "016121", 
 #                     "016202", "016215", "016226", "016252", "017044", "017079", "017082", 
 #                     "017092", "017112", "017158", "017162", "017193", "018009", "018099", 
 #                     "018157", "019104", "020059", "023078", "025051", "025070", "025071", 
 #                     "025073", "026033", "028041", "030062", "030130", "031022", "034049", 
 #                     "035027", "037061", "040015", "041058", "041069", "041070", "042042", 
 #                     "043047", "043058", "045005", "046036", "047023", "047024", "048020", 
 #                     "048052", "049018", "050040", "050041", "051040", "051041", "052037", 
 #                     "057048", "058030", "058095", "058115", "060091", "061020", "061085", 
 #                     "062014", "063065", "064104", "065062", "065118", "065122", "065127", 
 #                     "065140", "065143", "066041", "067015", "067019", "067039", "068008", 
 #                     "069009", "070049", "072039", "076026", "078020", "078026", "078031", 
 #                     "078115", "079052", "079068", "079126", "079142", "080046", "080058", 
 #                     "080087", "081025", "082018", "082059", "082062", "082068", "083001", 
 #                     "083040", "090002", "090017", "090092", "091031", "091088", "094002", 
 #                     "094050", "095002", "095027", "095056", "095076", "095088", "096085", 
 #                     "098052", "109024"]
    
 #   istatCodeToFix = ["004246", "004250", "008035", "008054", "012115", "014062", "016201", 
 #                     "017087", "017175", "017191", "023037", "058088", "058098", "058113", 
 #                     "060060", "065151", "066108", "067020", "069078", "069093", "071051", 
 #                     "076087", "078063", "078140", "078148", "078153", "078155", "079143", 
 #                     "082078", "083103", "091100", "095075", "096014", "102037"]
    
    istatCodeToFix = ["001296", "058110", "079143", "090071"]

    recordCount = 0
    with TableServiceClient(endpoint=endpoint, credential=credential) as service:
        table = service.get_table_client(table_name=table_name)

        for istat in istatCodeToFix:
            found = False
            parameters = {"istat": istat}
            #name_filter = "PartitionKey eq 'org'"
            name_filter = "PaIdIstat eq '" + str(istat) + "'"
            queried_entities = table.query_entities(
                query_filter=name_filter
            )

            for entity in queried_entities:
                companyName = str(entity['CompanyName'])
                istatCompanyName = str("Comune di " + statsTable[istat][5])
                if companyName.lower() == istatCompanyName.lower():
                    entity['PaIdIstat'] = istat
                    table.update_entity(mode=UpdateMode.REPLACE, entity=entity)
                    recordCount += 1
                    found = True
                    break

            if not found:
                print("fix_istat_code|code not found: " + str(istat)) 

    print("fix_istat_code|record inserted: " + str(recordCount))


parser = argparse.ArgumentParser()


parser.add_argument('--istat', type=Path, required=True, help='Path to the \'istat\' CSV file.')
parser.add_argument('--env', type=str, required=True, choices=['local', 'd', 'u', 'p'],
                    help='Environment to upload the CSV')
parser.add_argument('--key', type=str, required=True, help='To to authenticate on Azure storage account')

args = parser.parse_args()


# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

# write config table to Azure table
fix_istat_code(args.env, f'{dirname}{args.istat}')