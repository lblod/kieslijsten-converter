#!/usr/bin/env ruby
# coding: utf-8
require 'linkeddata'
require 'date'
require 'securerandom'
require 'tempfile'
require 'csv'

class MandatenDb
  attr_reader :client, :log

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

  def initialize(endpoint)
    @client = SPARQL::Client.new(endpoint)
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
  end

  def write_ttl_to_file(name)
    output = Tempfile.new(name)
    begin
      output.write "# started #{name} at #{DateTime.now}"
      yield output
      output.write "# finished #{name} at #{DateTime.now}"
      output.close
      FileUtils.copy(output, File.join('./',"#{DateTime.now.strftime("%Y%m%d%H%M%S")}-#{name}.ttl"))
      output.unlink
    rescue StandardError => e
      puts e
      puts e.backtrace
      puts "failed to successfully write #{name}"
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
  def find_person(rrn)
    result = query(%(
      PREFIX adms:<http://www.w3.org/ns/adms#>
      PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
      SELECT ?person ?identifier
      WHERE {
        ?person adms:identifier ?identifier.
        ?identifier skos:notation "#{rrn}".
      }

    ))
    if result.size > 0
      [result[0][:person], result[0][:identifier]]
    else
      raise "person not found"
    end
  end
  def find_mandaat(orgaan, type)
    result = query(%(
          PREFIX org: <http://www.w3.org/ns/org#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    SELECT ?mandaat WHERE {
               <#{orgaan.to_s}> org:hasPost ?mandaat.
               ?mandaat org:role <#{type}>.
    }))
    if result.size > 0
      log.debug result[0][:mandaat].inspect
      result[0][:mandaat]
    else
      raise "wow, no mandaat found for #{orgaan}"
    end
  end

  def create_mandataris(persoon, mandaat, datum)
    graph = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("#{BASE_IRI}/mandatarissen/#{uuid}")
    graph << [ iri , RDF.type, MANDAAT.Mandataris ]
    graph << [ iri, MU.uuid, uuid ]
    graph << [ iri, ORG.holds, mandaat ]
    graph << [ iri, MANDAAT.start, Date.strptime(datum, "%m/%d/%Y")]
    graph << [ iri, MANDAAT.isBestuurlijkeAliasVan, persoon]
    [graph, iri]
  end
  def mandataris_exists(orgaan, type)
    query(%(
          PREFIX org: <http://www.w3.org/ns/org#>
          PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
           ASK {
               <#{orgaan.to_s}> org:hasPost ?mandaat.
               ?mandaat org:role <#{type}>.
               ?mandataris org:holds ?mandaat.
    }))
  end
end


mdb = MandatenDb.new(ENV['ENDPOINT'])
orgaantype=RDF::URI.new('http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/4955bd72cd0e4eb895fdbfab08da0284') # burgemeester
burgemeesterRole="http://data.vlaanderen.be/id/concept/BestuursfunctieCode/5ab0e9b8a3b2ca7c5e000013"
mdb.write_ttl_to_file("burgemeesters") do |file|
  mdb.read_csv('burgemeesters2019.csv') do |index, row|
    begin
      gemeentenaam = row["kieskring"]
      datum = row["datum eedaflegging"]
      rol = row["Mandaat"]
      if rol.downcase == "burgemeester" and not (datum.nil? || datum.empty?)
        orgaan = mdb.bestuursorgaan_voor_gemeentenaam(gemeentenaam, orgaantype, "2019-01-01" )
        unless mdb.mandataris_exists(orgaan, burgemeesterRole)
          puts "creating burgemeester voor #{gemeentenaam}"
          (persoon, identifier) = mdb.find_person(row['RR'])
          burgemeester = mdb.find_mandaat(orgaan, burgemeesterRole)
          (mandataris, iri) = mdb.create_mandataris(persoon, burgemeester, datum)
        end
        file.write mandataris.dump(:ttl)
      end
    rescue StandardError => e
      puts e
    end
  end
end
