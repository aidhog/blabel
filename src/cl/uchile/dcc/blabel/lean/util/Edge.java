package cl.uchile.dcc.blabel.lean.util;

import org.semanticweb.yars.nx.Node;

public class Edge implements Comparable<Edge> {
	int hc = 0; 
	
	final Node predicate;
	final Node value;
	final boolean out;
	
	public Edge(Node predicate, Node value, boolean out){
		this.predicate = predicate;
		this.value = value;
		this.out = out;
	}

	@Override
	public int hashCode() {
		if(hc == 0){
			final int prime = 31;
			int result = 1;
			result = prime * result + (out ? 1231 : 1237);
			result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
			hc = prime * result + ((value == null) ? 0 : value.hashCode());
		}
		return hc;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Edge other = (Edge) obj;
		if (out != other.out)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		if (predicate == null) {
			if (other.predicate != null)
				return false;
		} else if (!predicate.equals(other.predicate))
			return false;
		return true;
	}

	@Override
	public int compareTo(Edge obj) {
		if(this == obj)
			return 0;
		if(obj==null)
			return Integer.MIN_VALUE;
		
		// cheap check
		int comp = Boolean.compare(out, obj.out);
		if(comp!=0){
			return comp;
		}
		
		// most likely to be different
		if(value!=obj.value){
			if(value==null){
				return Integer.MIN_VALUE;
			} else if(obj.value==null){
				return Integer.MAX_VALUE;
			} else{
				comp = value.compareTo(obj.value);
				if(comp!=0){
					return comp;
				}
			}
		}
		
		// last but not least
		if(predicate!=obj.predicate){
			if(predicate==null){
				return Integer.MIN_VALUE;
			} else if(obj.predicate==null){
				return Integer.MAX_VALUE;
			} else{
				comp = predicate.compareTo(obj.predicate);
			}
		}
		return comp;
	}

	@Override
	public String toString() {
		return "Edge [predicate=" + predicate + ", value=" + value + ", out=" + out + "]";
	}
}
