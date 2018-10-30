#!/usr/bin/env ruby
# coding: utf-8
require 'linkeddata'
require 'date'
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

  def create_gebied(naam)
    triples = RDF::Repository.new
    gebied_uuid = SecureRandom.uuid
    gebied = RDF::URI.new("http://data.lblod.info/id/bestuurseenheden/#{gebied_uuid}")
    triples << [gebied, RDF.type, PROV.Location]
    triples << [gebied, MU.uuid, gebied_uuid]
    triples << [gebied, EXT.werkingsgebiedNiveau, "Gemeente"]
    triples << [gebied, RDFS.label, naam]
    [gebied, triples]
  end

  def create_gemeente(gemeente, code, gebied)
    triples = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("http://data.lblod.info/id/bestuurseenheden/#{uuid}")
    triples << [iri, RDF.type, RDF::URI.new('http://data.vlaanderen.be/ns/besluit#Bestuurseenheid') ]
    triples << [iri, MU.uuid, uuid]
    triples << [iri, SKOS.prefLabel, gemeente ]
    triples << [iri, BESLUIT.classificatie, RDF::URI.new('http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000001')]
    triples << [iri, BESLUIT.werkingsgebied, gebied]
    triples << [iri, EXT.kbonummer, code]
    triples << [iri, DC.identifier, code]
    {
      'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/4955bd72cd0e4eb895fdbfab08da0284': 'Burgemeester',
     'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000005': 'Gemeenteraad',
     'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000006': "College van Burgemeester en Schepenen" }.each do |klasse, title|
       orgaan_uuid = SecureRandom.uuid
       orgaan = RDF::URI.new("http://data.lblod.info/id/bestuursorganen/#{orgaan_uuid}")
       triples << [orgaan, RDF.type, BESLUIT.Bestuursorgaan ]
       triples << [orgaan, SKOS.prefLabel, "#{title} #{gemeente}" ]
       triples << [orgaan, MU.uuid, orgaan_uuid]
       triples << [orgaan, BESLUIT.bestuurt, iri]
       triples << [orgaan, BESLUIT.classificatie, RDF::URI.new(klasse.to_s)]
       tijdsorgaan_uuid = SecureRandom.uuid
       tijdsorgaan = RDF::URI.new("http://data.lblod.info/id/bestuursorganen/#{tijdsorgaan_uuid}")
       triples << [tijdsorgaan, RDF.type, BESLUIT.Bestuursorgaan ]
       triples << [tijdsorgaan, MU.uuid, tijdsorgaan_uuid]
       triples << [tijdsorgaan, MANDAAT.isTijdspecialisatieVan, orgaan]
       triples << [tijdsorgaan, MANDAAT.bindingStart, Date.parse("2019-01-01")]
     end
    triples
  end

  def create_ocmw(gemeente, code, gebied)
    triples = RDF::Repository.new
    uuid = SecureRandom.uuid
    iri = RDF::URI.new("http://data.lblod.info/id/bestuurseenheden/#{uuid}")
    triples << [iri, RDF.type, RDF::URI.new('http://data.vlaanderen.be/ns/besluit#Bestuurseenheid') ]
    triples << [iri, MU.uuid, uuid]
    triples << [iri, SKOS.prefLabel, gemeente ]
    triples << [iri, BESLUIT.classificatie, RDF::URI.new('http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000002')]
    triples << [iri, BESLUIT.werkingsgebied, gebied]
    triples << [iri, EXT.kbonummer, code]
    triples << [iri, DC.identifier, code]
    {
      'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/53c0d8cd-f3a2-411d-bece-4bd83ae2bbc9': "Voorzitter van het Bijzonder Comité voor de Sociale Dienst",
      'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000007': "Raad voor Maatschappelijk Welzijn",
      'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000008': "Vast Bureau",
      'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000009': "Bijzonder Comité voor de Sociale Dienst"
 }.each do |klasse, title|
       orgaan_uuid = SecureRandom.uuid
       orgaan = RDF::URI.new("http://data.lblod.info/id/bestuursorganen/#{orgaan_uuid}")
       triples << [orgaan, RDF.type, BESLUIT.Bestuursorgaan ]
       triples << [orgaan, SKOS.prefLabel, "#{title} #{gemeente}" ]
       triples << [orgaan, MU.uuid, orgaan_uuid]
       triples << [orgaan, BESLUIT.bestuurt, iri]
       triples << [orgaan, BESLUIT.classificatie, RDF::URI.new(klasse.to_s)]
       tijdsorgaan_uuid = SecureRandom.uuid
       tijdsorgaan = RDF::URI.new("http://data.lblod.info/id/bestuursorganen/#{tijdsorgaan_uuid}")
       triples << [tijdsorgaan, RDF.type, BESLUIT.Bestuursorgaan ]
       triples << [tijdsorgaan, MU.uuid, tijdsorgaan_uuid]
       triples << [tijdsorgaan, MANDAAT.isTijdspecialisatieVan, orgaan]
       triples << [tijdsorgaan, MANDAAT.bindingStart, Date.parse("2019-01-01")]
     end
    triples
  end
    def fusiegemeenten
    write_ttl_to_file('fusiegemeenten') do |rep|
      {
        Kruisem: { gemeente: "0697608954", ocmw: "0697663788" },
        Lievegem: { gemeente: "0697609152", ocmw: "0697663986" },
        Pelt: { gemeente: "0697609350" , ocmw: "0697664976"},
        "Puurs-Sint-Amands": { gemeente: "0697609548" , ocmw: "0697665075"},
        Oudsbergen: { gemeente: "0697609251" , ocmw: "0697664382"}
      }.each do |gemeente, codes|
        gemeente = gemeente.to_s
        (gebied, triples) = create_gebied(gemeente)
        rep.write(triples.dump(:ttl))
        triples = create_gemeente(gemeente, codes[:gemeente], gebied)
        rep.write(triples.dump(:ttl))
        triples = create_ocmw(gemeente, codes[:ocmw], gebied)
        rep.write(triples.dump(:ttl))
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
      FileUtils.copy(output, File.join('./',"#{DateTime.now.strftime("%Y%m%dT%H%M%S")}-#{name}.ttl"))
      output.unlink
    rescue StandardError => e
      puts e
      puts "failed to successfully write #{name}"
      output.close
      output.unlink
    end
    end
end

MandatenDb.new.fusiegemeenten
