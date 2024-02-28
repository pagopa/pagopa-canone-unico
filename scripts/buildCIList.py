import argparse
import csv
from pathlib import Path
import os

def load_ec_table(filePathEC, filePathFilters, filePathOut, filePathMissingCf):

    print("load_ec_table|loading file [" + filePathEC + "]")
    filename = open(filePathEC, 'r')
    file = csv.reader(filename, delimiter=';')
    
    # skip the headers line
    next(file, None)

    # creating empty dictionary
    ecDict = {}

    # iterating over each row
    print("load_ec_table|Cycling over file rows")
    for row in file:
        if (not row[2] in ecDict):
            ecDict[row[2]] = [row[0], row[1], row[2],row[3],row[4],row[5],
                              row[6],row[7],row[8],row[9],row[10],row[11],
                              row[12],row[13],row[14],row[15],row[16],row[17],
                              row[18],row[19],row[20],row[21],row[22],row[23],
                              row[24],row[25],row[26],row[27],row[28],row[29],
                              row[30],row[31],row[32]]
        
    print("load_ec_table|file [" + filePathEC + "] loaded")

    # loading filter table
    print("load_ec_table|loading file [" + filePathFilters + "]")
    filename = open(filePathFilters, 'r')
    file = csv.reader(filename, delimiter=';')
    
    # skip the headers line
    next(file, None)

    # creating empty dictionary
    ecFilteredTable = []
    ecHeaders = []
    
    # iterating over each row
    print("load_ec_table|Cycling over file rows")
    # add header
    ecHeaders.append("denominazioneEnte")
    ecHeaders.append("codAmm")
    ecHeaders.append("codiceFiscale")
    ecHeaders.append("dataAdesione")
    ecHeaders.append("codiceGs1Gln")
    ecHeaders.append("cognomeRp")
    ecHeaders.append("nomeRp")
    ecHeaders.append("codiceFiscaleRp")
    ecHeaders.append("mailRp")
    ecHeaders.append("telefonoRp")
    ecHeaders.append("cellulareRp")
    ecHeaders.append("tipoIntermediazione")
    ecHeaders.append("denominazioneIntermediarioPartner")
    ecHeaders.append("cognomeRt")
    ecHeaders.append("nomeRt")
    ecHeaders.append("codiceFiscaleRt")
    ecHeaders.append("mailRt")
    ecHeaders.append("telefonoRt")
    ecHeaders.append("cellulareRt")
    ecHeaders.append("statoConnessione")
    ecHeaders.append("modello")
    ecHeaders.append("dataCollaudo")
    ecHeaders.append("dataPreEsercizio")
    ecHeaders.append("dataEsercizio")
    ecHeaders.append("auxDigit")
    ecHeaders.append("codiceSegregazione")
    ecHeaders.append("applicationCode")
    ecHeaders.append("codiceInterbancario")
    ecHeaders.append("idStazione")
    ecHeaders.append("statoAssociazione")
    ecHeaders.append("dataStatoAssociazione")
    ecHeaders.append("versioneStazione")
    ecHeaders.append("flagBroadcast")

    ecFilteredTable.append(ecHeaders)
    missingCf = []
    for row in file:
        if (row[0] in ecDict):
            ecFilteredTable.append(ecDict[row[0]])
        else:
            print("load_ec_table|WARNING fiscal code " + row[0] + " not found")
            missingCf.append([row[0]])      

    if (len(missingCf) > 0):
        with open(filePathMissingCf, 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(missingCf)

    print("load_ec_table|table loaded in memory")

    # write table to csv
    print("load_ec_table|writing table to csv file")
    with open(filePathOut, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(ecFilteredTable)

    print("load_ec_table|csv file completed")


parser = argparse.ArgumentParser()

parser.add_argument('--ec', type=Path, required=True, help='Path to the \'export_ec\' CSV file.')
parser.add_argument('--ecToFilter', type=Path, required=True, help='Path to the \'lista_comuni\' CSV file.')
parser.add_argument('--outputPath', type=Path, required=True, help='Path to the \'output\' CSV file.')
parser.add_argument('--missingCI', type=Path, required=True, help='Path to the \'missing Creditor Institution\' CSV file.')

args = parser.parse_args()


# retrieve the base path dir to deal with input/output file
dirname = os.path.dirname(__file__)

export_ec = load_ec_table(f'{dirname}{args.ec}', f'{dirname}{args.ecToFilter}', f'{dirname}{args.outputPath}', f'{dirname}{args.missingCI}')