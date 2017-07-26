package cl.uchile.dcc.blabel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;

import cl.uchile.dcc.blabel.cli.LabelRDFGraph;
import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.blabel.label.GraphLabelling;

public class BlabelTest {

	@Test
	public void testGraphLabellingWithUriOutput() throws InterruptedException, HashCollisionException {

		String[] input = new String[] { "_:a <p> _:b .\n",
				"_:b <p> _:c .\n",
				"_:c <p> _:a .\n",
				"_:x <p> _:y .\n",
				"_:y <p> _:z .\n",
				"_:z <p> _:x .\n",
				"<u> <p> <v> .\n" };

		List<String> goldStandard = Arrays.asList(new String[] { "<u> <p> <v> .",
				"<urn:blabel:SK0036cc1684b47a2ab9d572712e9bf6d4b6> <p> <urn:blabel:SK003e2cfe64ac8abefd0daec5da3bda7422> .",
				"<urn:blabel:SK003e2cfe64ac8abefd0daec5da3bda7422> <p> <urn:blabel:SK00eea00e382802360d4926a9d2a3d2648a> .",
				"<urn:blabel:SK00b18eb44df51d9d026169a1751071c678> <p> <urn:blabel:SK00d9c27c19e1a5093ebd351941c8bd1664> .",
				"<urn:blabel:SK00c122c4399935a5128591dd7d68d9b640> <p> <urn:blabel:SK00b18eb44df51d9d026169a1751071c678> .",
				"<urn:blabel:SK00d9c27c19e1a5093ebd351941c8bd1664> <p> <urn:blabel:SK00c122c4399935a5128591dd7d68d9b640> .",
				"<urn:blabel:SK00eea00e382802360d4926a9d2a3d2648a> <p> <urn:blabel:SK0036cc1684b47a2ab9d572712e9bf6d4b6> ." });

		NxParser iter = new NxParser(Arrays.asList(input).iterator());

		// load the graph into memory
		Collection<Node[]> stmts = new ArrayList<Node[]>();
		boolean bnode = false;
		while (iter.hasNext()) {
			Node[] triple = iter.next();
			if (triple.length >= 3) {
				stmts.add(new Node[] { triple[0], triple[1], triple[2] });
				bnode = bnode | (triple[0] instanceof BNode) | (triple[2] instanceof BNode);
			} else {
				fail("Not a triple " + Nodes.toN3(triple));
			}
		}

		final List<String> actual = new ArrayList<String>();

		LabelRDFGraph.labelGraph(stmts, new Callback() {

			@Override
			public void startDocument() {
			}

			@Override
			public void endDocument() {
			}

			@Override
			public void processStatement(Node[] nx) {
				String triple = Nodes.toN3(nx);
				System.out.println(triple);
				actual.add(triple);
			}
		}, new GraphLabelling.GraphLabellingArgs(), "urn:blabel:", false);

		Collections.sort(goldStandard);
		Collections.sort(actual);

		assertEquals(goldStandard, actual);

	}

	@Test
	public void testGraphLabellingWithBnodeOutput() throws InterruptedException, HashCollisionException {

		String[] input = new String[] { "_:a <p> _:b .\n",
				"_:b <p> _:c .\n",
				"_:c <p> _:a .\n",
				"_:x <p> _:y .\n",
				"_:y <p> _:z .\n",
				"_:z <p> _:x .\n",
				"<u> <p> <v> .\n" };

		List<String> goldStandard = Arrays.asList(new String[] { "<u> <p> <v> .",
				"_:SK0036cc1684b47a2ab9d572712e9bf6d4b6 <p> _:SK003e2cfe64ac8abefd0daec5da3bda7422 .",
				"_:SK003e2cfe64ac8abefd0daec5da3bda7422 <p> _:SK00eea00e382802360d4926a9d2a3d2648a .",
				"_:SK00b18eb44df51d9d026169a1751071c678 <p> _:SK00d9c27c19e1a5093ebd351941c8bd1664 .",
				"_:SK00c122c4399935a5128591dd7d68d9b640 <p> _:SK00b18eb44df51d9d026169a1751071c678 .",
				"_:SK00d9c27c19e1a5093ebd351941c8bd1664 <p> _:SK00c122c4399935a5128591dd7d68d9b640 .",
				"_:SK00eea00e382802360d4926a9d2a3d2648a <p> _:SK0036cc1684b47a2ab9d572712e9bf6d4b6 ." });

		NxParser iter = new NxParser(Arrays.asList(input).iterator());

		// load the graph into memory
		Collection<Node[]> stmts = new ArrayList<Node[]>();
		boolean bnode = false;
		while (iter.hasNext()) {
			Node[] triple = iter.next();
			if (triple.length >= 3) {
				stmts.add(new Node[] { triple[0], triple[1], triple[2] });
				bnode = bnode | (triple[0] instanceof BNode) | (triple[2] instanceof BNode);
			} else {
				fail("Not a triple " + Nodes.toN3(triple));
			}
		}

		final List<String> actual = new ArrayList<String>();

		LabelRDFGraph.labelGraph(stmts, new Callback() {

			@Override
			public void startDocument() {
			}

			@Override
			public void endDocument() {
			}

			@Override
			public void processStatement(Node[] nx) {
				String triple = Nodes.toN3(nx);
				System.out.println(triple);
				actual.add(triple);
			}
		}, new GraphLabelling.GraphLabellingArgs(), "", true);

		Collections.sort(goldStandard);
		Collections.sort(actual);

		assertEquals(goldStandard, actual);

	}
}
