package cl.uchile.dcc.blabel.lean.util;

import java.util.TreeSet;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class NodeBindCount implements Comparable<NodeBindCount>{
	private final Node n;
	private final int i;
	private final boolean self;
	
	public NodeBindCount(Node n, int i, boolean self){
		this.n = n;
		this.i = i;
		this.self = self;
	}


	/**
	 * Returns IRIs/Literals first
	 * Otherwise orders on bind count
	 * Otherwise orders on node value
	 */
	@Override
	public int compareTo(NodeBindCount arg0) {
		if(arg0==this)
			return 0;
		if(arg0==null)
			return 1;
		
		boolean gn1 = n instanceof BNode;
		boolean gn2 = arg0.n instanceof BNode;
		
		int comp = Boolean.compare(gn1, gn2);
		if(comp!=0)
			return comp;
	
		comp = arg0.i - i;
		if(comp!=0)
			return comp;
		
		comp = Boolean.compare(self, arg0.self);
		if(comp!=0)
			return comp;
		
		return n.compareTo(arg0.n);
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i;
		result = prime * result + ((n == null) ? 0 : n.hashCode());
		result = prime * result + (self ? 1231 : 1237);
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeBindCount other = (NodeBindCount) obj;
		if (i != other.i)
			return false;
		if (n == null) {
			if (other.n != null)
				return false;
		} else if (!n.equals(other.n))
			return false;
		if (self != other.self)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodeBindCount [n=" + n + ", i=" + i + ", self=" + self + "]";
	}

	public Node getNode() {
		return n;
	}

	public int getI() {
		return i;
	}

	public boolean isSelf() {
		return self;
	}
	
	// quick debug test
	public static void main(String[] args) throws Exception{
		NodeBindCount nbc1 = new NodeBindCount(new BNode("b1"),2,false);
		NodeBindCount nbc2 = new NodeBindCount(new BNode("b2"),2,true);
		NodeBindCount nbc3 = new NodeBindCount(new Resource("r"),2,false);
		TreeSet<NodeBindCount> nbcs = new TreeSet<NodeBindCount>();
		nbcs.add(nbc1);
		nbcs.add(nbc2);
		nbcs.add(nbc3);
		
		System.err.println(nbcs);
	}
}
