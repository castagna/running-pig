
triples = LOAD './src/test/resources/nquads.ttl' USING PigStorage('\t') AS (s,p,o,g);

-- dump triples;

STORE triples INTO './target/output/' USING PigStorage('\t');