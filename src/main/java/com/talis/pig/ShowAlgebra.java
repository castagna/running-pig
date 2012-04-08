package com.talis.pig;

import java.io.IOException;
import java.io.StringWriter;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;

public class ShowAlgebra {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Query query = QueryFactory.read("query.rq");
//		PrintUtils.printOp(IndentedWriter.stdout, query, true);

		Op op = Algebra.compile(query);
		
		StringWriter out = new StringWriter();
		op.visit(new PigLatinWriter(out));
		// out.close();
		
		System.out.println(out.toString());

	}

}
