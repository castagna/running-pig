REGISTER ./target/running-pig-0.0.1.jar;

triples = LOAD './src/test/resources/nquads.ttl' USING com.talis.pig.NQuadsStorage() AS (g,s,p,o);

a = FILTER triples BY ( p == '<http://xmlns.com/foaf/0.1/name>' )

STORE a INTO './target/output' USING com.talis.pig.NQuadsStorage();