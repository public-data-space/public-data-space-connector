## Public Data Space Connector

### Prepare connector
1. Clone and build the backend  \
a) `git clone https://github.com/public-data-space/public-data-space-connector.git` \
b) ` mvn clean package `
2. Clone and build the zenodo adapter \
a) ` git clone https://github.com/public-data-space/public-data-space-zenodo-adapter.git` \
b) ` mvn clean package `
3. Clone and build the frontend \
a) ` git clone  https://github.com/public-data-space/public-data-space-ui.git ` \
b) ` npm install `

### Start connector without docker-compose
1. Start the database 
` docker run --name ids -p 5432:5432 -e POSTGRES_PASSWORD=ids -e POSTGRES_USER=ids -e POSTGRES_DB=ids postgres  `
2. Start the backend (inside the public-data-space-connector directory) 
` java -jar .\target\public-data-space-connector-2.1.0-fat.jar `
3. Start the zenodo adapter (inside the public-data-space-zenodo-adapter directory)
 ` java -jar .\target\public-data-space-zenodo-adapter-1.1.0-fat.jar `
4. Start the frontend (inside the public-data-space-ui)
` npm run dev `

Frontend is available at localhost:8081/browse (username:admin, password:admin)

Note: make sure the following ports are free:
|  | Connector | UI | Zenodo-adapter | Database |
|---|---|---|---|---|
| URL | localhost:8080 | localhost:8081 | localhost:8070 | localhost:5432 |

### Start connector with docker-compose

Run the following commands (inside the public-data-space-connector directory, make sure all other repositories are at the same level)
1. ` docker-compose -f docker-compose_build.yml up -d db ` 
2. ` docker-compose -f docker-compose_build.yml up -d public-data-space-connector ` 
3. ` docker-compose -f docker-compose_build.yml up -d public-data-space-zenodo-adapter ` 
4. ` docker-compose -f docker-compose_build.yml up -d public-data-space-ui ` 
5. ` docker-compose -f docker-compose_build.yml up -d nginx ` 

Frontend is available at localhost/browse (username:admin, password:admin)
