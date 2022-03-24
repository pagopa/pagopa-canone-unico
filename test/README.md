# Blob Utils

List, Upload and Download files for Canone Unico on Azure Blob Storage.

## Prerequisites

Before to use this tool, type this command:

`az login`

---

## List

To show the list of the files in a blob storage. Default list the input container of develop.

`python3 ./blob_utils.py list`

Optional

```
--account-name {name of storage account}
--container-name {name of the container}
```

### Examples

`python3 ./blob_utils.py list --account-name pagopadcanoneunicosa --container-name pagopadcanoneunicosaincsvcontainer`

---

## Download

To download a file from a blob storage.

`python3 ./blob_utils.py download --file {name of the file in the blob storage}`

**Required**

`--file {name of the file to upload}`

Optional

```
--path {path where save the file}
--account-name {name of storage account}
--container-name {name of the container}
```

### Examples

`python3 ./blob_utils.py download --file filename.csv --path out --account-name pagopadcanoneunicosa --container-name pagopadcanoneunicosaincsvcontainer`

---

## Upload

To upload a file in the blob storage. If `rows` is set it creates a CSV file with fake data first.

`python3 ./blob_utils.py upload --file {name of the file to upload}`

**Required**

`--file {name of the file to upload}`

Optional

```
--rows {numbers of rows to generate in the CSV}
--path {local path where find the CSV}
--account-name {name of storage account}
--container-name {name of the container}
```

### Examples

- with auto generation of the file (with --rows parameter)

`python3 ./blob_utils.py upload --file filename.csv --rows 5 --path in --account-name pagopadcanoneunicosa --container-name pagopadcanoneunicosaincsvcontainer`

- to upload an existing file (without --rows parameter)

`python3 ./blob_utils.py upload --file filename.csv --path in --account-name pagopadcanoneunicosa --container-name pagopadcanoneunicosaincsvcontainer`
