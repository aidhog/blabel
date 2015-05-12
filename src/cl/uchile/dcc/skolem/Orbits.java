package cl.uchile.dcc.skolem;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;

public class Orbits {
	
	// only non-trivial ones
	Partition<Node> orbits = new Partition<Node>();
	
	public Orbits(){
		super();
	}
	
	public int countOrbits(){
		return orbits.partitions;
	}
	
	public int maxOrbit(){
		int max = 0;
		for(TreeSet<Node> o:orbits.values()){
			if(o.size()>max)
				max = o.size();
		}
		return max;
	}
	
	public boolean addAndCompose(HashMap<Node,Node> auto){
		return updateOrbits(auto, this.orbits);
	}
	
	public TreeSet<Node> getNonTrivialOrbit(Node n){
		return orbits.getPartition(n);
	}
	
	private boolean updateOrbits(HashMap<Node,Node> auto, Partition<Node> orbits){
		boolean add = false;
		for(Map.Entry<Node, Node> e:auto.entrySet()){
			if(!e.getKey().equals(e.getValue())){
				add |= orbits.addPair(e.getKey(), e.getValue());
			}
		}
		return add;
	}
	
	/**
	 * See if the given nodes form an orbit in the known automorphisms.
	 * 
	 * @param nodes
	 * @return
	 */
	public boolean isOrbit(Collection<Node> nodes){
		if(nodes==null || nodes.isEmpty()){
			throw new IllegalArgumentException("Expecting non-null, non-empty input for orbit detection");
		}
		Iterator<Node> i = nodes.iterator();
		
		TreeSet<Node> o = orbits.get(i.next());
		if(o==null)
			return false;
		return o.containsAll(nodes);
	}
	
	
	public static HashMap<Node,Node> inverse(HashMap<Node,Node> auto){
		 HashMap<Node,Node> inv = new HashMap<Node,Node>(auto.size());
		 for(Map.Entry<Node, Node> e : auto.entrySet()){
			 inv.put(e.getValue(), e.getKey());
		 }
		 return inv;
	}
	
	public static HashMap<Node,Node> comp(HashMap<Node,Node> a1, HashMap<Node,Node> a2){
		HashMap<Node,Node> comp = new HashMap<Node,Node>(a1.size());
		for(Map.Entry<Node, Node> e : a1.entrySet()){
			 Node c = a2.get(e.getValue());
			 if(c==null){
				 throw new IllegalArgumentException("Not an automorphism");
			 }
			 comp.put(e.getKey(), c);
		}
		return comp;
	}
}
