package cl.uchile.dcc.skolem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;

import com.google.common.hash.HashCode;

/**
 * Maintains an ordered partition of nodes, referred to as the
 * refinement. Nodes can be distinguished as the process progresses,
 * splitting sets in the partition. This is necessary when the initial
 * colouring doesn't distinguish all nodes. 
 * 
 * @author Aidan
 *
 */
public class RefinablePartition {
//	HashMap<HashNode,TreeSet<HashNode>> partition;
	
	ArrayList<TreeSet<Node>> rParts = new ArrayList<TreeSet<Node>>();
	
	public RefinablePartition(Collection<? extends Node> nodes){
		// stores an ordered partition called
		// a refinement
		rParts = new ArrayList<TreeSet<Node>>();
		
		// initially add all nodes to one set in the refinement
		TreeSet<Node> all = new TreeSet<Node>();
		all.addAll(nodes);
		rParts.add(all);
	}
	
	
	public ArrayList<TreeSet<Node>> getCurrentRefinement(){
		return rParts;
	}
	
	/**
	 * Take a new partition of nodes (e.g., computed from colouring) and
	 * compute the refinement.
	 *  
	 * @param parts
	 * @return has the refinement changed?
	 */
	public boolean refine(Partition<Node> parts, Map<Node,HashCode> hc){
		return refine(parts, hc, null);
	}
	
	/**
	 * Take a new partition of nodes (e.g., computed from colouring) and
	 * compute the refinement. Takes a node that was manually distinguished
	 * which will be considered first in the refinement order.
	 * 
	 * @param parts The new partition
	 * @param distinguished A node that was manually distinguished
	 * @return has the refinement changed?
	 */
	public boolean refine(Partition<Node> parts, Map<Node,HashCode> hc, Node distinguished){
		if(parts.countPartitions() == rParts.size()){
			// the refinement and the partition are the same
			return false;
		}
		
		// the new refinement
		ArrayList<TreeSet<Node>> rPartsNew = new ArrayList<TreeSet<Node>>();
		
		for(int i=0; i<rParts.size(); i++){
			TreeSet<Node> rPart = rParts.get(i);
			
			// the smallest acts as a representative
			Node small = rPart.first();
			TreeSet<Node> part = parts.getPartition(small);
			
			if(part.size() != rPart.size()){
				// some thing new has been split
				
				// use this to order the new splits
				// orders by size of splits, then by hash
				TreeSet<TreeSet<Node>> splits = new TreeSet<TreeSet<Node>>(new RefinementComparator(hc));
				
				// is there a distinguished node in this split?
				boolean d = false;
				
				for(Node n: rPart){
					// for every node in the refinement in the
					// corresponding partition
					if(distinguished!=null && n.equals(distinguished)){
						// mark if it's the distinguished
						d = true;
					}
					// otherwise add and order the new partition
					else splits.add(parts.getPartition(n));
				}
				
				// add the manually distinguished node first
				if(d){
					TreeSet<Node> split = new TreeSet<Node>();
					split.add(distinguished);
					rPartsNew.add(split);
				}
				
				// add the new partition parts in order
				for(TreeSet<Node> split:splits){
					rPartsNew.add(split);
				}
			} else{
				// if partitions are the same size, 
				// move the partition set over unchanged to the new
				// refinement
				rPartsNew.add(rPart);
			}
		}
		
		rParts = rPartsNew;
		return true;
	}
	
	/**
	 * Gets the mapping from one refinement to another.
	 * Useful for finding automorphisms. Assumes complete
	 * refinements with no sets greater than 1.
	 * 
	 * @param rp1
	 * @param rp2
	 * @return
	 */
	public static HashMap<Node,Node> getMapping(RefinablePartition rp1, RefinablePartition rp2){
		HashMap<Node,Node> m = new HashMap<Node,Node>();
		
		if(rp1.rParts.size() != rp2.rParts.size()){
			throw new IllegalArgumentException("Must pass partitions of the same size");
		}
		
		for(int i=0; i<rp1.rParts.size(); i++){
			TreeSet<Node> s1 = rp1.rParts.get(i);
			TreeSet<Node> s2 = rp2.rParts.get(i);
			
			if(s1.size()!=1 || s2.size()!=1){
				throw new IllegalArgumentException("Must pass complete refinements");
			}
			
			m.put(s1.first(), s2.first());
		}
		
		return m;
	}
	
	
	/**
	 * Orders by size, then by hash. Assumes two sets cannot have the same hash:
	 * hence its use is local. It's not a general set comparator and not safe to use
	 * elsewhere.
	 * 
	 * @author Aidan
	 *
	 */
	private static class RefinementComparator implements Comparator<TreeSet<Node>>{
		Map<Node,HashCode> hashes = null;
		
		public RefinementComparator(Map<Node,HashCode> hashes){
			this.hashes = hashes;
		}
		
		public int compare(TreeSet<Node> o1, TreeSet<Node> o2) {
			int diff = o1.size() - o2.size();
			if(diff!=0) return diff;
			
			HashCode hc1 = hashes.get(o1.first());
			HashCode hc2 = hashes.get(o2.first());
			
			if(hc1==null && hc2==null)
				return 0;
			if(hc1==null)
				return -1;
			if(hc2==null)
				return 1;
			
			return hc1.toString().compareTo(hc2.toString());
			
			// we assume here that different hashes must imply different
			// splits ... so this is sufficient
		}
		
	}
}
