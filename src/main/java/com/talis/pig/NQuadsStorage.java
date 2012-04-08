package com.talis.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.openjena.atlas.lib.SinkNull;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.lang.ParserFactory;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.out.SinkQuadOutput;
import com.hp.hpl.jena.sparql.core.Quad;

public class NQuadsStorage implements LoadFunc, StoreFunc {

	private long end;
	private LangNQuads parser = null;
	private BufferedPositionedInputStream is = null;
	private SinkQuadOutput out = null;
	private NodeEncoder encoder = null;

	public NQuadsStorage() {
		encoder = new NodeEncoder();
	}

	@Override
	public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
		this.is = is;
		this.end = end;
		this.parser = ParserFactory.createParserNQuads(is, new SinkNull<Quad>());

	}

	@Override
	public DataBag bytesToBag(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public String bytesToCharArray(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Double bytesToDouble(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Float bytesToFloat(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Integer bytesToInteger(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Long bytesToLong(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Map<String, Object> bytesToMap(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Tuple bytesToTuple(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Tuple getNext() throws IOException {
		if (is == null || is.getPosition() > end) {
			return null;
		}

		if (!parser.hasNext()) {
			return null;
		}

		try {
			Quad quad = parser.next();
			Tuple tuple = TupleFactory.getInstance().newTuple(4);
			if (quad.isTriple()) {
				tuple.set(0, Quad.tripleInQuad);
			} else {
				tuple.set(0, encoder.toString(quad.getGraph()));
			}
			tuple.set(1, encoder.toString(quad.getSubject()));
			tuple.set(2, encoder.toString(quad.getPredicate()));
			tuple.set(3, encoder.toString(quad.getObject()));

			return tuple;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public Schema determineSchema(String fileName, ExecType execType, DataStorage storage) throws IOException {
		return null;
	}

	@Override
	public RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFieldList) throws FrontendException {
		return new LoadFunc.RequiredFieldResponse(false);
	}

	@Override
	public void bindTo(OutputStream os) throws IOException {
		out = new SinkQuadOutput(os);
	}

	@Override
	public void finish() throws IOException {
		out.flush();
		out.close();
	}

	@Override @SuppressWarnings("unchecked")
	public Class getStorePreparationClass() throws IOException {
		return null;
	}

	@Override
	public void putNext(Tuple tuple) throws IOException {
		Quad quad = null;
		Node s = encoder.toNode((String) tuple.get(1));
		Node p = encoder.toNode((String) tuple.get(2));
		Node o = encoder.toNode((String) tuple.get(3));
		if ( tuple.get(0) == null ) {
			quad = new Quad(Quad.tripleInQuad, s, p, o);			
		} else {
			Node g = encoder.toNode((String) tuple.get(0));
			quad = new Quad(g, s, p, o);			
		}
		out.send(quad);
	}

}
