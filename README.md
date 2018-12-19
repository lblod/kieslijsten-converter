# kieslijsten converter
create the appropriate data structures based on provided data

## configuring this converter
1. place the required data in `data/db/toLoad`.

 - person uri's with a link to their identifier (rrn)
 - previously generated administrative bodies for the new legislature
 - administrative units, including links to administrative bodies
 - mandates, linked to their respective administrative bodies
 

2. place the required source data in `data/input`
 -  `lijsten.csv` a csv with candidate lists per township (required headers: `kieskring`, `lijstnr`, `lijst`, `datum` )
 - `kandidaten.csv` a csv with candidates per list per township (required headers:  `kieskring`, `lijstnr`, `verkregen zetels`, `volgnr`, `Rrvoornaam `, `RRachternaam`, `RR`, `verkozen`, `opvolger`, `naamstemmen`, `geslacht`, `geboortedatum`)

3. add any necessary transformation queries in `data/transforms`. 
These queries will run after the input has been loaded, queries should have a `.rq` extension

4. configure the env variables:
- `PERSON_ENCRYPTION_SALT`:encryption salt for persons
- `ENDPOINT`: SPARQL endpoint to connect to ('http://database:8890/sparql')
- `KANDIDATENLIJST_TYPE_IRI`: iri of the type of the candidates list ('http://data.vlaanderen.be/id/concept/KandidatenlijstLijsttype/95de36e5-8c7a-4308-af7b-75afbd943dd2')
-  `BESTUURSORGAAN_TYPE_IRI`: iri of the type of administrative body the 'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000005'  # gemeenteraad
- `ORGAAN_START_DATUM`:  start date of the elected administrative body
- `LOG_LEVEL`: "info" or "debug"
- `INPUT_DATE_FORMAT`: date format used in the CSV's ("%d/%m/%Y")



## running this converter
```
  docker-compose up
```

the end result will be available in ./data/output/
* sensitive data will be written to ./data/output/[date]-[type]-sensitive.ttl
* other data will be written to ./data/output/[date]-[type].ttl


## extra scripts
- create-fusiegemeenten.rb

#### import-burgemeester.rb
```ENDPOINT=http://localhost:8890/sparql ruby import-burgemeester.rb```
needs a burgemeesters2019.csv on the same path and a endpoint with all data from editor (including identifiers)
