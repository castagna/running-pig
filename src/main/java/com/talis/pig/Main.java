package com.talis.pig;

import java.io.IOException;

import org.apache.pig.PigServer;

public class Main {

	public static void main(String[] args) throws IOException {
		PigServer pig = new PigServer("local");
		// pig.debugOn();
		pig.registerJar("./target/running-pig-0.0.1.jar");
		pig.registerQuery("triples = LOAD './src/test/resources/nquads.ttl' USING com.talis.pig.NQuadsStorage() AS (g,s,p,o);");
		pig.registerQuery("a = FILTER triples BY ( p == '<http://xmlns.com/foaf/0.1/name>' ) ;");
		pig.registerQuery("b = FILTER a BY ( o == '\"Bob\"' ) ;");
		pig.store("b", "./target/output", "com.talis.pig.NQuadsStorage()");
	}

}
