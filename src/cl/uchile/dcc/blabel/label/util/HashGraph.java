package cl.uchile.dcc.blabel.label.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashGraph {
	// the hash function used for the graph
	private final HashFunction hf;
	
	// a map from original IRIs and literals to hashed versions
	private final HashMap<Node,HashCode> staticHashes;
	
	// a map from original blank nodes to dynamic hashes
	private HashMap<Node,HashCode> dynamicHashes;
	
	// the hashed triples
	private final ArrayList<Node[]> data;
	
	// the initial blank hash
	private final HashCode blankHash;
	
//	// cache hashcode
//	private int hashCode = 0;
//	
//	// cache graph hash until something updated
//	private HashCode graphHash = null;
//	
//	// cache ground graph hash until something updated
//	private HashCode groundGraphHash = null;
	
	/**
	 * Create a blank HashGraph
	 * @param hf The hashing function to be used
	 */
	public HashGraph(HashFunction hf){
		this(hf, new ArrayList<Node[]>(), new HashMap<Node,HashCode>(), hf.hashString("", Charsets.UTF_8), new HashMap<Node,HashCode>());
	}
	
	// used mainly for the branch copy
	private HashGraph(HashFunction hf, ArrayList<Node[]> data, HashMap<Node,HashCode> staticHashes, HashCode blankHash, HashMap<Node,HashCode> dynamicHashes){
		this.hf = hf;
		this.staticHashes = staticHashes;
		this.blankHash = blankHash;
		this.data = data;
		this.dynamicHashes = dynamicHashes;
	}
	
	/**
	 * The initial hash used for blank nodes ... the hash of 
	 * the empty string
	 * @return
	 */
	public HashCode getBlankHash(){
		return blankHash;
	}

	/**
	 * Adds a triple to the graph.
	 * Be aware that it doesn't check for duplicates.
	 * That's your job.
	 * 
	 * @param triple
	 */
	public void addTriple(Node[] stmt){
//		clearCachedObjects();
		
		if(stmt.length<3){
			throw new IllegalArgumentException("Expecting triples not tuples of length "+stmt.length);
		}
		
		Node[] triple = new Node[3];
		
		for(int i=0; i<triple.length; i++){
			triple[i] = stmt[i];
			getOrCreateHashCode(stmt[i]);
			
		}
		data.add(triple);
	}
	
//	/**
//	 * Clear objects cached for efficiency when the
//	 * object state changes.
//	 * 
//	 * Messy since state can be changed outside 
//	 * but helps with runtimes.
//	 */
//	public void clearCachedObjects(){
//		graphHash = null;
////		hashCode = 0;
//		groundGraphHash = null;
//	}
//	
	/**
	 * Produces a cheap copy of the graph, where immutable objects
	 * (graph structure, static hashes, hash function) are copied by pointer, 
	 * whereas dynamic objects (dynamic hashes) are shallow-copied
	 * @return
	 */
	public HashGraph branch(){
		HashMap<Node,HashCode> dh = new HashMap<Node,HashCode>();
		dh.putAll(dynamicHashes);
		HashGraph hg = new HashGraph(hf, data, staticHashes, blankHash, dh);
		return hg;
	}
	
	/**
	 * Get the HashNode containing the current hash for the node.
	 * 
	 * @param n
	 * @return
	 */
	public HashCode getHash(Node n){
		if(n instanceof BNode){
			return dynamicHashes.get(n);
		}
		return staticHashes.get(n);
	}
	
	/**
	 * Get the HashNode containing the current hash for the blank node.
	 * 
	 * @param n
	 * @return
	 */
	public HashCode getHash(BNode n){
		return dynamicHashes.get(n);
	}
	
	private HashCode getOrCreateHashCode(Node n){
		HashCode hc = getHash(n);
		if(hc==null){
//			clearCachedObjects();
			
			if(n instanceof BNode){
				hc = blankHash;
				dynamicHashes.put(n, hc);
			} else{
				hc = hf.hashString(n.toN3(), Charsets.UTF_8);
				staticHashes.put(n,hc);
			}
		}
		return hc;
	}
	
	public HashFunction getHashFunction(){
		return hf;
	}
	
	public HashMap<Node,HashCode> getBlankNodeHashes(){
		return dynamicHashes;
	}
	
	public HashCode getGraphHash(){
		HashCode b = blankHash;
		for(Node[] t: data){
			ArrayList<HashCode> tup = new ArrayList<HashCode>();
			for(Node n:t){
				tup.add(getHash(n));
			}
			HashCode o = Hashing.combineOrdered(tup);

			tup.clear();
			tup.add(o);
			tup.add(b);

			b = Hashing.combineUnordered(tup);
		}
		return b;
	}
	
	/**
	 * Hash all blank nodes with the mux
	 * @param mux
	 * @return
	 */
	public static void muxHash(HashGraph hg, HashCode mux){
		HashSet<Node> bnodes = new HashSet<Node>();
		bnodes.addAll(hg.getBlankNodeHashes().keySet());

		for(Node b:bnodes){
			HashCode hc = hg.getHash(b);
			ArrayList<HashCode> tup = new ArrayList<HashCode>(2);
			tup.add(hc);
			tup.add(mux);
			HashCode comb = Hashing.combineOrdered(tup);
			hg.getBlankNodeHashes().put(b, comb);
		}
//		hg.clearCachedObjects();
	}
	
	public HashCode getGroundSubGraphHash(){
		HashCode b = blankHash;
		for(Node[] t: data){
			if(!(t[0] instanceof BNode) && !(t[2] instanceof BNode)){
				ArrayList<HashCode> tup = new ArrayList<HashCode>();
				for(Node n:t){
					tup.add(getHash(n));
				}
				HashCode o = Hashing.combineOrdered(tup);

				tup.clear();
				tup.add(o);
				tup.add(b);

				b = Hashing.combineUnordered(tup); 
			}
		}
		return b;
	}
	
	public void updateBNodeHashes(HashMap<Node,HashCode> bnodeHashes){
		dynamicHashes.putAll(bnodeHashes);
//		clearCachedObjects();
	}
	
	public void setBNodeHashes(HashMap<Node,HashCode> bnodeHashes){
		dynamicHashes = bnodeHashes;
//		clearCachedObjects();
	}
	
	public ArrayList<Node[]> getData(){
		return data;
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		
		for(Node[] triple:data){
			for(Node n: triple){
				buf.append(n.toN3()+"#"+getHash(n)+" ");
			}
			buf.append(".\n");
		}
		return buf.toString();
	}
	
	/**
	 * Splits the hashgraph into multiple where each
	 * graph represents all triples for each set of connected blank nodes 
	 * (blank nodes appearing in the same triple).
	 * 
	 * Ground triples are removed (if previously present).
	 * 
	 * Shallow copy: colours are preserved.
	 * 
	 * @return
	 */
	public Collection<HashGraph> blankNodePartition(){
		Partition<Node> part = new Partition<Node>();
		
		// first create a partition of blank nodes based
		// on being connected
		for(Node[] t: data){
			if(t[0] instanceof BNode && t[2] instanceof BNode && !t[0].equals(t[2])){
				part.addPair(t[0], t[2]);
			}
		}
		
		HashMap<Node,HashGraph> pivotToGraph = new HashMap<Node,HashGraph>();
		for(Node[] t: data){
			// doesn't matter which we pick, both are in the
			// same partition
			BNode b = null;
			if(t[0] instanceof BNode){
				b = (BNode)t[0];
			} else if(t[2] instanceof BNode){
				b = (BNode)t[2];
			} else{
				continue;
			}
			
			// use the lowest bnode in the partition
			// to map to its graph
			TreeSet<Node> bp = part.getPartition(b);
			Node pivot = null;
			
			if(bp == null)
				pivot = b; // singleton ... unconnected blank node
			else pivot = bp.first();
			
			HashGraph hg = pivotToGraph.get(pivot);
			if(hg == null){
				hg = new HashGraph(hf);
				pivotToGraph.put(pivot, hg);
			}
			
			hg.addTriple(t);
		}
		
		
		return pivotToGraph.values();
	}
	
//	@Override
//	public int hashCode() {
//		if(hashCode==0){
////			final int prime = 31;
////			int result = 1;
////			result = prime * result + ((blankHash == null) ? 0 : blankHash.hashCode());
////			result = prime * result + ((data == null) ? 0 : data.hashCode());
////			result = prime * result + ((dynamicHashes == null) ? 0 : dynamicHashes.hashCode());
////			result = prime * result + hashCode;
////			result = prime * result + ((hf == null) ? 0 : hf.hashCode());
////			result = prime * result + ((staticHashes == null) ? 0 : staticHashes.hashCode());
//			hashCode = getGraphHash().hashCode();
//		}
//		return hashCode;
//	}
//
//	@Override
//	/**
//	 * The equals method is modulo isomorphism and thus must be used with care.
//	 * In fact, this is a bit of a hack and could lead to collisions, but it 
//	 * is better than just relying on getGraphHash(), which it seems will collide
//	 * more often than it should. :(
//	 * 
//	 * This is used in GraphLabelling to detect isomorphic graphs
//	 * 
//	 * Two HashGraphs are considered equals if the graph hashes are equal, the
//	 * data sizes are equal, the number of blank nodes is equal, and the static
//	 * hash objects (their contents) are equal
//	 */
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		HashGraph other = (HashGraph) obj;
//		if(!other.getGraphHash().equals(getGraphHash()))
//			return false;
//		if (data == null) {
//			if (other.data != null)
//				return false;
//		} else if (data.size() != other.data.size())
//			return false;
//		if (dynamicHashes == null) {
//			if (other.dynamicHashes != null)
//				return false;
//		} else if (dynamicHashes.size() != other.dynamicHashes.size())
//			return false;
//		if (staticHashes == null) {
//			if (other.staticHashes != null)
//				return false;
//		} else if (!staticHashes.equals(other.staticHashes))
//			return false;
//		return true;
//	}
	
	
}
