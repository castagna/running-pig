package com.talis.pig;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.riot.ParserFactory;
import com.hp.hpl.jena.riot.lang.LangNQuads;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphMem;
import com.hp.hpl.jena.tdb.lib.SinkQuadsToDataset;

public class NQuadsStorageTest {
	
	private PigServer pig = null;

	@Before
	public void setUp() throws ExecException, IOException {
		this.pig = new PigServer("local");
	}
	
	@After
	public void tearDown() {
		this.pig.shutdown();
		this.pig = null;
	}
	
	@Test
	public void testNQuadsStorage() throws IOException {
		DatasetGraph expected = load ( "src/test/resources", "nquads.ttl" );
		 
		pig.registerJar("./target/running-pig-0.0.1.jar");
		pig.registerQuery("quads = LOAD './src/test/resources/nquads.ttl' USING com.talis.pig.NQuadsStorage() AS (g,s,p,o);");
		pig.store("quads", "./target/output", "com.talis.pig.NQuadsStorage()");

		DatasetGraph actual = load ( "target", "output" );

		Iterator<Node> graphs = expected.listGraphNodes();
		while (graphs.hasNext()) {
			Node graph = graphs.next();
			assertTrue ( actual.containsGraph(graph) );
			assertTrue ( expected.getGraph(graph).isIsomorphicWith(actual.getGraph(graph)) );
		}
	}

	private DatasetGraph load(String path, String filename) throws FileNotFoundException {
		FileInputStream in = new FileInputStream( new File (path, filename) );
		DatasetGraph dataset = new DatasetGraphMem();
		LangNQuads parser = ParserFactory.createParserNQuads(in, new SinkQuadsToDataset(dataset));
		parser.parse();
		
		return dataset;
	}
}
