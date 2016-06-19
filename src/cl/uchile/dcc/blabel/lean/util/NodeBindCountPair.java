package cl.uchile.dcc.blabel.lean.util;

public class NodeBindCountPair  implements Comparable<NodeBindCountPair> {
	private final NodeBindCount s;
	private final NodeBindCount o;
	private final boolean sBigger;
	
	public NodeBindCountPair(NodeBindCount s, NodeBindCount o){
		// if they're equal, increment bind count by 1
		if(s.equals(o)){
			this.s = new NodeBindCount(s.getNode(),s.getI()+1,s.isSelf());
			this.o = new NodeBindCount(o.getNode(),o.getI()+1,o.isSelf());
			sBigger = false;
		} else{
			this.s = s;
			this.o = o;
			sBigger = (s.compareTo(o)>0);
		}
		
		
		// if they are the same, increment
		// bind count by 1
		
	}

	@Override
	public int compareTo(NodeBindCountPair c) {
		if(c==this)
			return 0;
		if(c==null)
			return 1;
			
		NodeBindCount max = (sBigger) ? s : o;
		NodeBindCount maxC = (c.sBigger) ? c.s : c.o;
		int comp = max.compareTo(maxC);
		if(comp!=0){
			return comp;
		}
		
		NodeBindCount min = (sBigger) ? o : s;
		NodeBindCount minC = (c.sBigger) ? c.o : c.s;
		comp = min.compareTo(minC);
		if(comp!=0){
			return comp;
		}
		
		return Boolean.compare(sBigger, c.sBigger);
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((o == null) ? 0 : o.hashCode());
		result = prime * result + ((s == null) ? 0 : s.hashCode());
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
		NodeBindCountPair other = (NodeBindCountPair) obj;
		if (o == null) {
			if (other.o != null)
				return false;
		} else if (!o.equals(other.o))
			return false;
		if (s == null) {
			if (other.s != null)
				return false;
		} else if (!s.equals(other.s))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "NodeBindCountPair [s=" + s + ", o=" + o + "]";
	}

	public NodeBindCount getSubject() {
		return s;
	}

	public NodeBindCount getObject() {
		return o;
	}
	
}
