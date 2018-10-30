#!/usr/bin/env ruby

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
          SELECT ?iri
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



  def create_kandidatenlijst(lijstnaam:, lijstnr:, lijsttype:, behoortTot: )
    graph = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("#{BASE_IRI}/kandidatenlijsten/#{uuid}")
    graph << [iri, RDF.type, MANDAAT.Kandidatenlijst]
    graph << [iri, MU.uuid, uuid]
    graph << [iri, MANDAAT.behoortTot,behoortTot]
    graph << [iri, MANDAAT.lijsttype, lijsttype]
    graph << [iri, SKOS.prefLabel, lijstnaam]
    [iri, graph]
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
end

class Converter
  attr_reader :log, :client, :mdb, :output_path, :input_path, :transform_path, :input_date_format
  def initialize(endpoint: , transform_path: , input_path:, output_path:, input_date_format:, log_level: "info")
    @endpoint = endpoint
    @client = SPARQL::Client.new(endpoint)
    @input_path = input_path
    @output_path = output_path
    @transform_path = transform_path
    @log = Logger.new(STDOUT)
    @log.level = log_level == "info" ? Logger::INFO : Logger::DEBUG
    @input_date_format = input_date_format
    @mdb = MandatenDb.new(endpoint, @log)
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

  def parse_kandidaten
    rrn_graph = RDF::Repository.new
    write_ttl_to_file('personen') do |repository|
      read_csv(File.join(input_path,'kandidaten.csv')) do |row|
        
      end
    end
    write_ttl_to_file('personen-sensitive') do |repository|
      repository.write(rrn_graph.dump(:ttl))
    end
  end

  def parse_kieslijsten(lijsttype_iri, bestuursorgaan_iri, orgaan_start_datum)
    verkiezing_cache = {}
    write_ttl_to_file('rechtstreekse-verkiezingen-en-kandidatenlijsten') do |repository|
      read_csv(File.join(input_path,'kandidaten.csv')) do |index, row|
        gemeentenaam = row["kieskring"]
        lijsttype = RDF::URI.new(lijsttype_iri)
        orgaantype = RDF::URI.new(bestuursorgaan_iri)
        begin
          orgaan = mdb.bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, orgaan_start_datum )
          date =  DateTime.strptime(row["datum"], input_date_format)
          if verkiezing_cache[orgaan]
            verkiezing = verkiezing_cache[orgaan]
          else
            ( verkiezing, graph ) = mdb.create_rechtstreekse_verkiezing(datum: date, stelt_samen: orgaan  )
            repository.write(graph.dump(:ttl))
            verkiezing_cache[orgaan]=verkiezing
          end
          ( lijst, graph ) = mdb.create_kandidatenlijst(lijstnaam: row["lijst"], lijstnr: row["lijstnr"], lijsttype: lijsttype, behoortTot: verkiezing)
          repository.write(graph.dump(:ttl))
        rescue StandardError => e
          log.warn "skipping row #{index}: #{e.message}"
        end
      end
    end
  end

  def write_ttl_to_file(name)
    output = Tempfile.new(name)
    begin
      output.write "# started #{name} at #{DateTime.now}"
      yield output
      output.write "# finished #{name} at #{DateTime.now}"
      output.close
      FileUtils.copy(output, File.join(output_path,"#{name}.ttl"))
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


converter = Converter.new(
  endpoint: ENV["ENDPOINT"],
  input_path: '/data/input',
  output_path: '/data/output',
  transform_path: '/data/transforms',
  input_date_format: ENV['INPUT_DATE_FORMAT'],
  log_level: ENV['LOG_LEVEL']
)
converter.run_data_transforms
converter.parse_kieslijsten(ENV['KANDIDATENLIJST_TYPE_IRI'], ENV['BESTUURSORGAAN_TYPE_IRI'], ENV['ORGAAN_START_DATUM'])

