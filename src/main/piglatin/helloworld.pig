
quads = LOAD './src/test/resources/data.nq' USING RdfStorage() AS (s,p,o,g);

-- dump quads;

STORE quads INTO './target/output/' USING RdfStorage();