#!/usr/bin/env ruby
# coding: utf-8

STDOUT.sync = true

require 'csv'
require 'linkeddata'
require 'date'
require 'fileutils'
require 'logger'
require 'securerandom'
require 'tempfile'

class MandatenDb
  ORG = RDF::Vocab::ORG
  FOAF = RDF::Vocab::FOAF
  SKOS = RDF::Vocab::SKOS
  DC = RDF::Vocab::DC
  PROV = RDF::Vocab::PROV
  RDFS = RDF::Vocab::RDFS
  MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
  PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
  PERSOON = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/persoon#")
  MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
  BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
  EXT = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/ext/")
  ADMS = RDF::Vocabulary.new('http://www.w3.org/ns/adms#')
  BASE_IRI='http://data.lblod.info/id'

  attr_reader :client, :log

  def initialize(endpoint, log)
    @client = SPARQL::Client.new(endpoint)
    @log = log
  end

  def query(q)
    log.debug q
    @client.query(q)
  end

  def bestuursorgaan_voor_gemeentenaam(naam, type, date)
    @bestuursorgaan_cache ||= {}
    orgaan = @bestuursorgaan_cache.dig(naam, type, date)
    if orgaan
      return orgaan
    end
    r = query(%(
          PREFIX org: <http://www.w3.org/ns/org#>
          PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
          SELECT DISTINCT ?iri
          WHERE {
             ?iri a besluit:Bestuursorgaan ;
                  mandaat:isTijdspecialisatieVan ?orgaan;
                  mandaat:bindingStart "#{date}"^^<http://www.w3.org/2001/XMLSchema#date>.
             ?eenheid a besluit:Bestuurseenheid ;
                      skos:prefLabel "#{naam}".
             ?orgaan besluit:bestuurt ?eenheid;
                     besluit:classificatie <#{type}>. # will at some point become org:classification
          }
   ))
    if r.size == 0
      raise "geen bestuursorgaan gevonden voor #{naam}!"
    end
    if r.size > 1
      raise "meerdere bestuursorganen gevonden voor #{naam}!"
    end
    @bestuursorgaan_cache[naam] ||= {}
    @bestuursorgaan_cache[naam][type] ||= {}
    @bestuursorgaan_cache[naam][type][date] = r[0][:iri]
    @bestuursorgaan_cache[naam][type][date]
  end

  def update_kandidatenlijst(lijstnaam:, lijstnr:, behoortTot:, lijsttype:)
    res = query(%(
          PREFIX org: <http://www.w3.org/ns/org#>
          PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
          SELECT DISTINCT ?iri
          WHERE {
                ?iri a mandaat:Kandidatenlijst;
                   mandaat:behoortTot <#{behoortTot}>;
                   mandaat:lijsttype <#{lijsttype}>;
                   skos:prefLabel "#{lijstnaam}".
          }
    ))
    repo = RDF::Graph.new
    if res.size == 1
      repo << [ res[0][:iri], MANDAAT.lijstnummer, lijstnr ]
    else
      log.warn "no result found for #{lijstnaam} #{behoortTot}"
    end
    repo
  end

  def create_kandidatenlijst(lijstnaam:, lijstnr:, lijsttype:, behoortTot: )
    graph = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("#{BASE_IRI}/kandidatenlijsten/#{uuid}")
    graph << [iri, RDF.type, MANDAAT.Kandidatenlijst]
    graph << [iri, MU.uuid, uuid]
    graph << [iri, MANDAAT.behoortTot,behoortTot]
    graph << [iri, MANDAAT.lijsttype, lijsttype]
    graph << [iri, MANDAAT.lijstnr, lijstnr]
    graph << [iri, SKOS.prefLabel, lijstnaam]
    [iri, graph]
  end

  def find_verkiezing(date, orgaan)
    res = query(%(
          PREFIX org: <http://www.w3.org/ns/org#>
          PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
          SELECT DISTINCT ?iri
          WHERE {
                ?iri a mandaat:RechtstreekseVerkiezing;
                   mandaat:datum "#{date}"^^xsd:date;
                   mandaat:steltSamen <#{orgaan}>.
          }
   ))
    if res.size == 1
      res[0][:iri].value
    else
      log.warn "#{res.size} rechtstreekse verkiezingen found for #{orgaan} and #{date}"
    end
  end

  def create_rechtstreekse_verkiezing(datum:, geldigheid: nil, stelt_samen:)
    graph = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("#{BASE_IRI}/rechtstreekse-verkiezingen/#{uuid}")
    graph << [iri, RDF.type, MANDAAT.RechtstreekseVerkiezing]
    graph << [iri, MU.uuid, uuid]
    graph << [iri, MANDAAT.datum, RDF::Literal.new(datum)]
    graph << [iri, MANDAAT.steltSamen, stelt_samen]
    [iri, graph]
  end

  def find_person(rrn)
    result = query(%(
      PREFIX adms:<http://www.w3.org/ns/adms#>
      PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
      SELECT DISTINCT ?person ?identifier
      WHERE {
        ?person adms:identifier ?identifier.
        ?identifier skos:notation "#{rrn}".
      }

    ))
    if result.size == 1
      [result[0][:person], result[0][:identifier]]
    else
      log.warn "found #{result.size} persons for rrn #{rrn}"
      nil
    end
  end

  def birthdate(date)
    result = query(%(
      PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>
      SELECT DISTINCT ?birthdate
      WHERE {
        ?birthdate a persoon:Geboorte;
                   persoon:datum "#{date}"^^xsd:date
      }
    ))
    if (result.size == 1)
      return [result[0][:birthdate], RDF::Repository.new]
    else
      uuid = SecureRandom.uuid
      triples = RDF::Repository.new
      iri = RDF::URI.new("#{BASE_IRI}/geboorte/#{uuid}")
      triples << [iri, RDF.type, PERSOON.Geboorte ]
      triples << [iri, MU.uuid, uuid ]
      triples << [iri, PERSOON.datum, RDF::Literal.new(date)]
      return [iri, triples]
    end

  end

  def geslacht(g)
    if g == 'F' or g == 'V'
      RDF::URI.new('http://publications.europa.eu/resource/authority/human-sex/FEMALE')
    elsif g == 'M'
      RDF::URI.new('http://publications.europa.eu/resource/authority/human-sex/MALE')
    else
      raise "ongeldige geslachtscode"
    end
  end

  def update_person(person:, voornaam:, achternaam: ,geslacht: , geboortedatum:, uuid:)
    triples = RDF::Repository.new
    triples << [ person, RDF.type, PERSON.Person ]
    triples << [ person, MU.uuid, uuid ]
    if voornaam
      triples << [ person, PERSOON.gebruikteVoornaam , voornaam ]
    end
    triples << [ person, FOAF.familyName , achternaam ]
    if geboortedatum
      (birthdate_iri, birthdate_triples) = birthdate(geboortedatum)
      triples << [ person, PERSOON.heeftGeboorte, birthdate_iri ]
      triples << birthdate_triples
    end
    if (geslacht.kind_of?(RDF::URI))
      triples << [ person, PERSOON.geslacht, geslacht ]
    else
      triples << [ person, PERSOON.geslacht, geslacht(geslacht) ]
    end
    triples
  end

  def fetch_person_details(person)
   result =  query(%(
          PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
          PREFIX persoon: <http://data.vlaanderen.be/ns/persoon#>
          PREFIX skos:    <http://www.w3.org/2004/02/skos/core#>
          PREFIX mu:      <http://mu.semte.ch/vocabularies/core/>
          SELECT DISTINCT ?voornaam ?achternaam ?geboortedatum ?geslacht ?uuid
          WHERE {
             OPTIONAL { <#{person.value}> foaf:familyName ?achternaam. }
             OPTIONAL { <#{person.value}> persoon:gebruikteVoornaam ?voornaam. }
             OPTIONAL { <#{person.value}> mu:uuid ?uuid. }
             OPTIONAL { <#{person.value}> persoon:geslacht ?geslacht. }
             OPTIONAL { <#{person.value}> persoon:heeftGeboorte/persoon:datum ?geboortedatum. }
          }
   ))
   if result.size === 1
     result[0]
   else
     { voornaam: nil, achternaam: nil, geslacht:nil, geboortedatum: nil, uuid: nil}
   end
  end

  def create_identifier(rrn)
    identifier_uuid = SecureRandom.uuid
    identifier = RDF::URI.new("#{BASE_IRI}/identificatoren/#{identifier_uuid}")
    sensitive_triples = RDF::Repository.new
    sensitive_triples << [ identifier, RDF.type, ADMS.Identifier ]
    sensitive_triples << [ identifier, SKOS.notation, rrn ]
    sensitive_triples << [ identifier, MU.uuid, identifier_uuid ]
    [identifier, sensitive_triples]
  end

  def create_person( identifier:, voornaam:, achternaam:, geslacht: , geboortedatum: )
    sensitive_triples = RDF::Repository.new
    triples = RDF::Repository.new
    person_uuid = SecureRandom.uuid
    person = RDF::URI.new("#{BASE_IRI}/personen/#{person_uuid}")

    sensitive_triples << [ person, ADMS.identifier, identifier ]
    triples << [ person, RDF.type, PERSON.Person ]
    triples << [ person, MU.uuid, person_uuid ]
    if voornaam
      triples << [ person, PERSOON.gebruikteVoornaam , voornaam ]
    end
    triples << [ person, FOAF.familyName , achternaam ]
    if geboortedatum
      (birthdate_iri, birthdate_triples) = birthdate(geboortedatum)
      triples << [ person, PERSOON.heeftGeboorte, birthdate_iri ]
      triples << birthdate_triples
    end
    triples << [ person, PERSOON.geslacht, geslacht(geslacht) ]
    [person, triples, sensitive_triples]
  end

  def create_resultaat(persoon:,lijst:,naamstemmen:,gevolg:,rangorde:)
    triples = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("#{BASE_IRI}/verkiezingsresultaten/#{uuid}")
    triples << RDF.Statement(iri, RDF.type, MANDAAT.Verkiezingsresultaat)
    triples << RDF.Statement(iri, MU.uuid, uuid)
    triples << RDF.Statement(iri, MANDAAT.aantalNaamstemmen, naamstemmen)
    triples << RDF.Statement(iri, MANDAAT.gevolg, gevolg)
    if rangorde
      triples << RDF.Statement(iri, MANDAAT.plaatsRangorde, rangorde)
    end
    triples << RDF.Statement(iri, MANDAAT.isResultaatVan, persoon)
    triples << RDF.Statement(iri, MANDAAT.isResultaatVoor, lijst)
    [iri, triples]
  end
end

class Converter
  attr_reader :log, :client, :mdb, :output_path, :input_path, :transform_path, :input_date_format
  def initialize(endpoint: , transform_path: , input_path:, output_path:, input_date_format:, log_level: "info", provincie: false)
    @endpoint = endpoint
    @client = SPARQL::Client.new(endpoint)
    @input_path = input_path
    @output_path = output_path
    @transform_path = transform_path
    @log = Logger.new(STDOUT)
    @log.level = log_level == "info" ? Logger::INFO : Logger::DEBUG
    @input_date_format = input_date_format
    @mdb = MandatenDb.new(endpoint, @log)
    @provincie = provincie
  end

  def run_data_transforms
    wait_for_db
    locations = Dir.glob("#{@transform_path}/*.rq")
    # TODO might need sort
    locations.each do |file|
      log.info "Executing migration #{file}"
      query = File.open(file, 'r:UTF-8').read
      client.update(query)
    end
  end

  def wait_for_db
    until is_database_up?
      log.info "Waiting for database... "
      sleep 2
    end

    log.info "Database is up"
  end

  def is_database_up?
    begin
      location = URI(@endpoint)
      response = Net::HTTP.get_response( location )
      return response.is_a? Net::HTTPSuccess
    rescue Errno::ECONNREFUSED
      return false
    end
  end

  def int(i)
    begin
      Integer(i)
    rescue
      0
    end
  end

  def gevolg(row)
    if int(row['verkozen']) > 0
      RDF::URI.new('http://data.vlaanderen.be/id/concept/VerkiezingsresultaatGevolgCode/89498d89-6c68-4273-9609-b9c097727a0f')
    elsif int(row['opvolger']) > 0
      RDF::URI.new('http://data.vlaanderen.be/id/concept/VerkiezingsresultaatGevolgCode/4c713f09-1317-4860-bbbd-e8f7dfd78a2f')
    else
      RDF::URI.new('http://data.vlaanderen.be/id/concept/VerkiezingsresultaatGevolgCode/dc8169a2-0e20-495d-9e01-30ccc83422b8')
    end
  end

  def rangorde(row)
    if int(row['verkozen']) > 0
      Integer(row['verkozen'])
    elsif int(row['opvolger']) > 0
      Integer(row['opvolger'])
    else
      nil
    end
  end

  def parse_kandidaten(lijsten)
    wait_for_db
    rrn_graph = RDF::Repository.new
    write_ttl_to_file('personen') do |repository|
      read_csv(File.join(input_path,'kandidaten.csv')) do |index, row|
        (persoon, identifier) = mdb.find_person(row['RR'])
        unless identifier
          (identifier, sensitive_triples) = mdb.create_identifier(row['RR'])
          rrn_graph << sensitive_triples
        end
        begin
          geboortedatum = Date.strptime(row["geboortedatum"], "%d/%m/%Y")
        rescue
          log.info "invalid date #{row["geboortedatum"]} for rrn: #{row["RR"]}, row: #{index} "
          geboortedatum = nil
        end
        unless row['RRvoornaam']
          log.info "missing RRvoornaam for rrn: #{row["RR"]}, row: #{index}"
        end
        if persoon
          gegevens = mdb.fetch_person_details(persoon)
          voornaam = gegevens[:voornaam] ? gegevens[:voornaam] : row['RRvoornaam']
          achternaam = gegevens[:achternaam] ? gegevens[:achternaam] : row['RRachternaam']
          geslacht = gegevens[:geslacht] ? gegevens[:geslacht] : row['geslacht']
          geboortedatum = gegevens[:geboortedatum] ? gegevens[:geboortedatum] : geboortedatum
          uuid = gegevens[:uuid] ? gegevens[:uuid] : persoon.value.sub("#{::MandatenDb::BASE_IRI}/personen/","")
          triples = mdb.update_person(person: persoon,
                            voornaam: voornaam,
                            achternaam: achternaam,
                            geslacht: geslacht,
                            geboortedatum: geboortedatum,
                            uuid: uuid)
          repository.write(triples.dump(:ttl))
        else
          log.warn "creating persoon for rrn #{row['RR']}"
          ( persoon, triples, sensitive_triples ) = mdb.create_person( identifier: identifier,
                                                                       voornaam: row['RRvoornaam'],
                                                                       achternaam: row['RRachternaam'],
                                                                       geslacht: row['geslacht'],
                                                                       geboortedatum: geboortedatum
                                                                     )
          rrn_graph << sensitive_triples
          repository.write(triples.dump(:ttl))
        end
        begin
          lijst = @provincie ? lijsten["#{provincie(row["NIS"])}-#{row["NIS"]}-#{row["lijstnr"]}"] : lijsten["#{row["kieskring"]}-#{row["lijstnr"]}"]
          log.debug lijst
          gevolg = gevolg(row)
          rangorde = rangorde(row)
          (resultaat, triples ) = mdb.create_resultaat(persoon: persoon,
                                                       lijst: lijst,
                                                       naamstemmen: Integer(row["naamstemmen"]),
                                                       gevolg: gevolg,
                                                       rangorde: rangorde
                                                      )
          triples << [ lijst, ::MandatenDb::MANDAAT.heeftKandidaat, persoon ]
          repository.write(triples.dump(:ttl))
        rescue StandardError => e
          log.error e.message
          log.warn "skipping row with nis code #{row["kieskring"]} and index {#{index}}"
        end
      end
    end
    write_ttl_to_file('personen-sensitive') do |repository|
      repository.write(rrn_graph.dump(:ttl))
    end
  end

  def update_kieslijsten(lijsttype_iri, bestuursorgaan_iri, orgaan_start_datum)
    wait_for_db
    lijsttype = RDF::URI.new(lijsttype_iri)
    orgaantype = RDF::URI.new(bestuursorgaan_iri)
    write_ttl_to_file('lijst_nr') do |repository|
      read_csv(File.join(input_path,'lijsten.csv')) do |index, row|
        gemeentenaam = row["kieskring"]
        orgaan = mdb.bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, orgaan_start_datum )
        date =  Date.strptime(row["datum"], input_date_format)
        verkiezing = mdb.find_verkiezing(date, orgaan)
        triples = mdb.update_kandidatenlijst(lijstnaam: row["lijst"], behoortTot: verkiezing, lijsttype: lijsttype, lijstnr: row["lijstnr"])
        repository.write triples.dump(:ttl)
      end
    end
  end

  def provincie(nis_code)
    case nis_code[0]
    when "1"
      "Antwerpen"
    when "2"
      "Vlaams-Brabant"
    when "3"
      "West-Vlaanderen"
    when "4"
      "Oost-Vlaanderen"
    when "7"
      "Limburg"
    else
      raise "invalid nis code #{nis_code}"
    end
  end

  def parse_kieslijsten(lijsttype_iri, bestuursorgaan_iri, orgaan_start_datum)
    wait_for_db
    verkiezing_cache = {}
    kieslijsten = {}
    write_ttl_to_file('rechtstreekse-verkiezingen-en-kandidatenlijsten') do |repository|
      read_csv(File.join(input_path,'lijsten.csv')) do |index, row|
        begin
          gemeentenaam = @provincie ? provincie(row['NIS']) : row["kieskring"]
          lijsttype = RDF::URI.new(lijsttype_iri)
          orgaantype = RDF::URI.new(bestuursorgaan_iri)
          orgaan = mdb.bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, orgaan_start_datum )
          date =  Date.strptime(row["datum"], input_date_format)

          if verkiezing_cache[orgaan]
            verkiezing = verkiezing_cache[orgaan]
          else
            ( verkiezing, graph ) = mdb.create_rechtstreekse_verkiezing(datum: date, stelt_samen: orgaan  )
            repository.write(graph.dump(:ttl))
            verkiezing_cache[orgaan]=verkiezing
          end
          ( lijst, graph ) = mdb.create_kandidatenlijst(lijstnaam: row["lijst"], lijstnr: row["lijstnr"], lijsttype: lijsttype, behoortTot: verkiezing)
          kieslijsten["#{gemeentenaam}-#{@provincie ? "#{row["NIS"]}-" : ""}#{row["lijstnr"]}"]=lijst
          if @provincie
            graph << [ lijst, MandatenDb::EXT.provinciedistrict, row["kieskring"] ]
          end
          repository.write(graph.dump(:ttl))
        rescue StandardError => e
          log.warn "skipping row #{index}: #{e.message}"
        end
      end
    end
    puts kieslijsten.inspect
    kieslijsten
  end

  def write_ttl_to_file(name)
    output = Tempfile.new(name)
    begin
      output.write "# started #{name} at #{DateTime.now}"
      yield output
      output.write "# finished #{name} at #{DateTime.now}"
      output.close
      FileUtils.copy(output, File.join(output_path,"#{DateTime.now.strftime("%Y%m%dT%H%M%S")}-#{name}.ttl"))
      output.unlink
    rescue StandardError => e
      log.error(e)
      log.error("failed to successfully write #{name}")
      output.close
      output.unlink
    end
  end

  def csv_parse_options
    { headers: :first_row, return_headers: true, encoding: 'UTF-8' }
  end

  def read_csv(file)
    headers_parsed = false
    index = 0
    begin
      ::CSV.foreach(file, csv_parse_options) do |row|
        unless headers_parsed
          @columnCount = row.size
          headers_parsed = true
          next
        end
        yield(index, row)
        index += 1
      end
    rescue ::CSV::MalformedCSVError => e
      log.error e.message
      log.error "parsing stopped after this error on index #{index}"
    end
  end
end

handling_province = ENV['KANDIDATENLIJST_TYPE_IRI'] == "http://data.vlaanderen.be/id/concept/KandidatenlijstLijsttype/90e3b7d0-2fae-43a1-957e-6daa8d072be1"

puts "Handling province: #{handling_province}"

converter = Converter.new(
  endpoint: ENV["ENDPOINT"],
  input_path: '/data/input/2025',
  output_path: '/data/output',
  transform_path: '/data/transforms/2025',
  input_date_format: ENV['INPUT_DATE_FORMAT'],
  log_level: ENV['LOG_LEVEL'],
  provincie: handling_province # only if we are parsing a provincielijst
)
converter.run_data_transforms
kieslijsten = converter.parse_kieslijsten(ENV['KANDIDATENLIJST_TYPE_IRI'], ENV['BESTUURSORGAAN_TYPE_IRI'], ENV['ORGAAN_START_DATUM'])
converter.parse_kandidaten(kieslijsten)
#converter.update_kieslijsten(ENV['KANDIDATENLIJST_TYPE_IRI'], ENV['BESTUURSORGAAN_TYPE_IRI'], ENV['ORGAAN_START_DATUM'])

