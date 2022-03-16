# Canone Unico

- [Canone Unico](#canone-unico)
    * [Technology Stack 📚](#technology-stack---)
    * [Start Project Locally 🚀](#start-project-locally---)
        + [Prerequisites](#prerequisites)
        + [Run docker container](#run-docker-container)
    * [Develop Locally 💻](#develop-locally---)
        + [Prerequisites](#prerequisites-1)
        + [Run the project](#run-the-project)
    * [Testing 🧪](#testing---)
        + [Prerequisites](#prerequisites-2)
        + [Unit testing](#unit-testing)
    * [Mainteiners 👨‍💻](#mainteiners------)

---

## Technology Stack 📚

- Java 11
- Azure functions

Canone Unico consists of 3 Azure functions:

- **CuCsvParsing** validates and parse the CSV content in the blob storage
- **CuCreateDebtPosition** calls [GPD](https://github.com/pagopa/pagopa-debt-position) service to create a debt position
  for each element in the CSV
- **CuGenerateOutputCsv** every day creates a report in the blob storage

![schema]()


---  

## Start Project Locally 🚀

### Prerequisites

- Docker
- (Optional) Python 3

### Run docker container

Under root folder typing:

```
docker run -t canone-unico
```

_NOTE: to create a **GPD mock** run in the mock directory:_ `python gpd.py`

---

## Develop Locally 💻

### Prerequisites

- [azure-functions-core-tools v3](https://docs.microsoft.com/it-it/azure/azure-functions/functions-run-local?tabs=v3%2Cwindows%2Ccsharp%2Cportal%2Cbash)
- [Azurite](https://docs.microsoft.com/it-it/azure/storage/common/storage-use-azurite?tabs=visual-studio)
- create a `local.setting.json` file (see: `local.setting.json.example`)
- (Optional) [Azure Storage Explorer](https://azure.microsoft.com/it-it/features/storage-explorer/)
- (Optional) Python 3

### Run the project

Under root folder typing:

`mvn azure-functions:run`

---

_NOTE: to create a **GPD mock** run in the mock directory:_ `python gpd.py`

## Testing 🧪

### Prerequisites

- maven
- [newman](https://www.npmjs.com/package/newman)
- [postman-to-k6](https://github.com/apideck-libraries/postman-to-k6)
- [k6](https://k6.io/)

### Unit testing

Under root folder typing:

`mvn clean verify`

---

## Mainteiners 👨‍💻

See `CODEOWNERS` file



