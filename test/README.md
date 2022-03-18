# Blob Utils

## List

Shows the list of the files in a blob storage.

`python3 ./blob_utils.py list`

Optional

```
--account-name {name of storage account}
--container_name {name of the container}
```

Example:

`python3 ./blob_utils.py list --account-name pagopadcanoneunicosa --container_name pagopadcanoneunicosaincsvcontainer`

## Download

Downloads a file from a blob storage.

`python3 ./blob_utils.py download --file {name of the file in the blob storage}`

Optional

```
--path {path where save the file}
--account-name {name of storage account}
--container_name {name of the container}
```

Example:

`python3 ./blob_utils.py download --file filename.csv --path out --account-name pagopadcanoneunicosa --container_name pagopadcanoneunicosaincsvcontainer`

## Upload

Uploads a file in the blob storage. If `rows` is set it creates a CSV file with fake data first.

`python3 ./blob_utils.py upload --file {name of the file to upload}`

Optional

```
--rows {numbers of rows to generate in the CSV}
--path {local path where find the CSV}
--account-name {name of storage account}
--container_name {name of the container}
```

Example:

`python3 ./blob_utils.py upload --file filename.csv --rows 5 --path in --account-name pagopadcanoneunicosa --container_name pagopadcanoneunicosaincsvcontainer`
