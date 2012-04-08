package com.talis.pig;

import java.nio.ByteBuffer;

import org.openjena.atlas.lib.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.tdb.nodetable.Nodec;
import com.hp.hpl.jena.tdb.nodetable.NodecSSE;

public class NodeEncoder {

	private Nodec nodec = null;
	
	public NodeEncoder() {
		nodec = new NodecSSE();
	}
	
	public ByteBuffer toByteBuffer(Node node) {
		ByteBuffer bb = nodec.alloc(node);
		nodec.encode(node, bb, null);

		return bb;
	}

	public Node toNode(ByteBuffer bb) {
		return nodec.decode(bb, null);
	}

	public String toString(Node node) {
		return Bytes.fromByteBuffer(toByteBuffer(node));
	}

	public Node toNode(String str) {
		ByteBuffer bb = ByteBuffer.allocate(4*str.length()) ;
		Bytes.toByteBuffer(str, bb);
		bb.flip();
		return toNode(bb);
	}

}
