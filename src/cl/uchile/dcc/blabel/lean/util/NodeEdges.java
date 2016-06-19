package cl.uchile.dcc.blabel.lean.util;

import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;

public class NodeEdges implements Comparable<NodeEdges> {
	final TreeSet<Edge> edges;
	final Node n;
	
	int hc = 0;
	
	public NodeEdges(Node n, TreeSet<Edge> edges){
		this.edges = edges;
		this.n = n;
	}
	
	public NodeEdges(Node n){
		this.edges = new TreeSet<Edge>();
		this.n = n;
	}
	
	public boolean addEdge(Edge e){
		boolean added = edges.add(e);
		if (added) hc = 0;
		return added;
	}

	@Override
	public int hashCode() {
		return n.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeEdges other = (NodeEdges) obj;
		if (n == null) {
			if (other.n != null)
				return false;
		} else if (!n.equals(other.n))
			return false;
		return true;
	}

	@Override
	public int compareTo(NodeEdges obj) {
		if(this == obj)
			return 0;
		if(obj==null)
			return Integer.MIN_VALUE;
		
		// most likely to be different
		if(edges!=obj.edges){
			if(n==null){
				return Integer.MIN_VALUE;
			} else if(obj.n==null){
				return Integer.MAX_VALUE;
			} else{
				return n.compareTo(obj.n);
			}
		}
		
		return 0;
		
	}

	@Override
	public String toString() {
		return "NodeEdges [edges=" + edges + ", n=" + n + "]";
	}

	public TreeSet<Edge> getEdges() {
		return edges;
	}

	public Node getNode() {
		return n;
	}		
}
