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
        - [Unit testing](#unit-testing)
        - [Integration testing](#integration-testing)
        - [Load testing](#load-testing)
    * [Mainteiners 👨‍💻](#mainteiners------)

---

## Technology Stack 📚

- Java 11
- Azure functions

---  

## Start Project Locally 🚀

### Prerequisites

- docker

### Run docker container

Under root folder typing:

```
docker run -t canone-unico
```

---

## Develop Locally 💻

### Prerequisites

- [azure-functions-core-tools v3](https://docs.microsoft.com/it-it/azure/azure-functions/functions-run-local?tabs=v3%2Cwindows%2Ccsharp%2Cportal%2Cbash)
- [Azurite](https://docs.microsoft.com/it-it/azure/storage/common/storage-use-azurite?tabs=visual-studio)
- create a `local.setting.json` file (see: `local.setting.json.example`)
- (Optional) [Azure Storage Explorer](https://azure.microsoft.com/it-it/features/storage-explorer/)

### Run the project

Under root folder typing:

`mvn azure-functions:run`

---

## Testing 🧪

### Prerequisites

- maven
- [newman](https://www.npmjs.com/package/newman)
- [postman-to-k6](https://github.com/apideck-libraries/postman-to-k6)
- [k6](https://k6.io/)

### Unit testing

Under `payments` folder typing:

`mvn clean verify`

---

## Mainteiners 👨‍💻

See `CODEOWNERS` file



