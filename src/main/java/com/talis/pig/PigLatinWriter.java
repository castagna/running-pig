package com.talis.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.List;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.util.FileUtils;

public class PigLatinWriter extends OpVisitorBase {

	private Writer out = null;
	private NodeEncoder encoder = new NodeEncoder();
	
	public PigLatinWriter(Writer out) {
		super();
		
		this.out = out;
	}
	
	public PigLatinWriter(OutputStream out) {
		this(FileUtils.asPrintWriterUTF8(out));
	}
	
    public void visit(OpBGP opBGP) {
    	try {
        	List<Triple> patterns = opBGP.getPattern().getList();
        	boolean first = true;
        	
        	for (Triple triple : patterns) {
        		String filter = null;

        		if (!triple.getSubject().isVariable()) {
        			if (first) filter = "FILTER dataset BY ";
        			filter.concat("( s == '" + out(triple.getSubject()) + "' )");
        			first = false;
        		} 
        		
        		if (!triple.getPredicate().isVariable()) {
        			if ( first ) {
            			filter = "FILTER dataset BY ";
        			} else {
        				filter = filter.concat(" AND ") ;
        			}
        			filter = filter.concat("( p == '" + out(triple.getPredicate()) + "' )");
        			first = false;        			
        		}

        		if (!triple.getObject().isVariable()) {
        			if ( first ) {
            			filter = "FILTER dataset BY ";
        			} else {
        				filter = filter.concat(" AND ") ;
        			}
        			filter = filter.concat("( o == '" + out(triple.getObject()) + "' )");
        			first = false;
        		}
        		
        		if (filter != null) {
        			out.write(filter);
        		}
			}
        	

		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    private String out(Node node) {
    	return encoder.toString(node);
    }
    
}
