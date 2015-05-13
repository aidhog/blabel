package cl.uchile.dcc.skolem;

import java.util.Iterator;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

/**
 * Maps blank nodes in the canonical graph
 * to URIs or blank nodes adding the given prefix.
 * @author Aidan
 *
 */
public class GraphLabelIterator implements Iterator<Node[]> {
	Iterator<Node[]> in;
	String prefix;
	boolean bnode;

	public GraphLabelIterator(Iterator<Node[]> in, String prefix, boolean bnode){
		this.in = in;
		this.prefix = prefix;
		this.bnode = bnode;
	}

	@Override
	public boolean hasNext() {
		return in.hasNext();
	}

	@Override
	public Node[] next() {
		Node[] next = in.next();
		Node[] nextc = new Node[next.length];
		System.arraycopy(next, 0, nextc, 0, next.length);

		nextc[0] = relabelBNode(nextc[0]);
		nextc[2] = relabelBNode(nextc[2]);
		return nextc;
	}

	private Node relabelBNode(Node b){
		if(b instanceof BNode){
			Node n = null;
			if(bnode){
				n = new BNode(prefix+b.toString());
			} else{
				n = new Resource(prefix+b.toString());
			}
			return n;
		} else return b;
	}

	@Override
	public void remove() {
		in.remove();
	}

}
