package cl.uchile.dcc.skolem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Class that does the main work. Runs colouring iterations and
 * traverses refinement tree while looking for pruning opportunities.
 * 
 * @author Aidan
 *
 */
public class GraphColouring implements Callable<GraphColouring.GraphResult> {
	public static final Comparator<Node[]> TRIPLE_COMP = NodeComparator.NC;
	public static final Comparator<TreeSet<Node[]>> GRAPH_COMP = new GraphComparator(NodeComparator.NC);

	public static final Level LOG_LEVEL = Level.INFO;
	public static final Logger LOG = Logger.getLogger(GraphColouring.class.getName());
	
	private static final int PRIME = 37;
	private static final int COLLISION_RECOVERY_ATTEMPTS = 5;
	
	static{
		for(Handler h : LOG.getParent().getHandlers()){
			if(h instanceof ConsoleHandler){
				h.setLevel(LOG_LEVEL);
			}
		} 
		LOG.setLevel(LOG_LEVEL);
	}

	public static final String BNODE_LABEL_PREFIX = "SK00";

	final HashGraph hg;
	boolean ran = false;
	MapTreeSet<HashCode,Node> part;
	ArrayList<Node> path;
	Leaves leaves = null; 
	ArrayList<Integer> colourIters;

	private RefinablePartition rfp = null;

	/**
	 * Will colour a HashGraph once run() is called.
	 * 
	 * @param hg
	 */
	public GraphColouring(HashGraph hg){
		this(hg,new ArrayList<Node>(),new Leaves(), new ArrayList<Integer>());
	}

	private GraphColouring(HashGraph hg, ArrayList<Node> path, Leaves leaves, ArrayList<Integer> colourIters){
		// the hashgraph contains the graph
		// and will be cloned when program
		// branches
		this.hg = hg;;
		
		// path so far
		// local to this object
		this.path = path;
		
		//leaves collected and shared
		// across all branches
		this.leaves = leaves;
		
		// the number of colouring iterations
				// run at each step, shared across all
				// branches
		this.colourIters = colourIters;
	}

	public GraphResult getCanonicalGraph(){
		return getCanonicalGraph(0);
	}
	
	public GraphResult getCanonicalGraph(int i){
		return getCanonicalGraph(hg.getHashFunction().hashInt(i));
	}

	/**
	 * Used to distinguish isomorphic sub-graphs to
	 * preserve "non-leanness" in final isomorphism.
	 * 
	 * Returns a graph and a hash code that uniquely
	 * identifies that graph (including the mux).
	 * @return
	 */
	public GraphResult getCanonicalGraph(HashCode mux){
		if(!leaves.isEmpty()){
			// any of the hash graphs in the first graph will do
			Map.Entry<TreeSet<Node[]>,ArrayList<GraphColouring>> c = leaves.firstEntry();
			GraphColouring gc = c.getValue().get(0);
			
			// compute the hash of the entire graph
			HashCode ghc = gc.hg.getGraphHash();
			
			// combine
			ArrayList<HashCode> tup = new ArrayList<HashCode>(2);
			tup.add(ghc);
			tup.add(mux);
			HashCode comb = Hashing.combineOrdered(tup);
			
			// clone the initial canonical graph
			HashGraph clone = gc.hg.branch();
			
			// mux the combined hashcode
			HashGraph.muxHash(clone, comb);
			
			// compute final graph
			TreeSet<Node[]> labelled = GraphColouring.labelBlankNodes(clone);

			// NOTE: the returned hash is not recomputed
			// since it is already unique
			return new GraphResult(labelled,clone,comb);
		}
		return null;
	}

	public void execute() throws InterruptedException, HashCollisionException{
		runColouring();

		if(part.size()!=hg.getBlankNodeHashes().size()){
			// if blank nodes are not distinguished by colour,
			// start manually distinguishing them
			traverse();
		}
	}

	private void traverse() throws InterruptedException, HashCollisionException{
		LOG.fine("Testing branch "+path);

		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		for(TreeSet<Node> set: rfp.getCurrentRefinement()){
			if(set.size()>1){				
				// first non-trivial part selected
				LOG.fine("Exploring part "+set);
				
				// siblings visited at this level
				ArrayList<Node> visited = new ArrayList<Node>();
				
				// cache of some orbits found at this level 
				Orbits orbits = null;
				for(Node n:set){
					boolean skip = false;
					
					
					// check to see if we can skip due to discovered automorphisms					
					if(visited.size()>0){
						// if we have already checked a sibling
						if(orbits==null){
							// create new cache of orbits for this path
							orbits = new Orbits();
						}
						// check if there is an orbit rooted for this
						// path which maps current node to a previously
						// visited sibling (checks cache of orbits first)
						skip = pruneSibling(n, visited, orbits);
						if(skip){
							LOG.fine("Skipping "+path+"->"+n+" ... found rooted automorphism");
						}
					}

					if(skip)
						continue;
					
					// copies the hashgraph by pointer for immutable stuff
					// creates new map for blank hashes
					HashGraph clone = hg.branch();

					// use the blank hash to distinguish the node
					HashMap<Node,HashCode> bhcs = clone.getBlankNodeHashes();
					HashCode hc = bhcs.get(n);

					// mark the selected blank node
					ArrayList<HashCode> tup = new ArrayList<HashCode>();
					tup.add(hc);
					tup.add(clone.getBlankHash());
					HashCode marked = Hashing.combineOrdered(tup);
					bhcs.put(n, marked);

					// this serves as the path of the tree
					// after the next branch
					ArrayList<Node> nextPath = new ArrayList<Node>();
					nextPath.addAll(this.path);
					nextPath.add(n);

					LOG.fine("Branching from "+this.path+" to "+nextPath);

					// re-run colouring / branch
					GraphColouring gc = new GraphColouring(clone,nextPath,leaves,colourIters);
					gc.execute();

					visited.add(n);
				}
				// we just need to look at the first non-trivial part
				break;
			}
		}
	}
	
	
	/**
	 * Checks to see if sibling node n needs to be pruned: check if it can
	 * be mapped by an automorphism (that roots the path to the sibling)
	 * to another sibling previously visited.
	 * 
	 * Orbits from a previous run with the same root can be considered as
	 * an optimisation. If a pruning ornit cannot be found, the orbits will be
	 * extended until a pruning step can be found or all orbits are exhaused
	 * for the current leaf graphs and root.
	 * 
	 * @param next
	 * @return
	 */
	private boolean pruneSibling(Node next, ArrayList<Node> visited, Orbits o){
		if(visited==null || visited.size()==0)
			return false;
		
		TreeSet<Node> orbits = o.getNonTrivialOrbit(next);
		if(orbits!=null && orbits.size()>0){
			for(Node v:visited){
				if(orbits.contains(v)){
					return true;
				}
			}
		}
		
		// first build a map of nodes to their index in the path
		HashMap<Node,Integer> index = new HashMap<Node,Integer>();
		int i = 0;
		for(Node d:path){
			index.put(d,i);
			i++;
		} 
		
		// gonna map index of path nodes to a colouring
		// once another colouring is found with same indexes, create the
		// isomorphism and add it to an orbit
		for(Map.Entry<TreeSet<Node[]>,ArrayList<GraphColouring>> iso:leaves.entrySet()){
			HashMap<ArrayList<Integer>,GraphColouring> rooted = new HashMap<ArrayList<Integer>,GraphColouring>();
			for(GraphColouring gc:iso.getValue()){
				ArrayList<Integer> indexes = new ArrayList<Integer>(index.size());
				for(int j=0; j<index.size(); j++){
					indexes.add(-1);
				}
				
				if(path.size()>0){
					i = 0;
					for(TreeSet<Node> ts: gc.rfp.rParts){
						for(Map.Entry<Node,Integer> d:index.entrySet()){
							if(ts.contains(d.getKey())){
								indexes.set(d.getValue(),i);
								continue;
							}
						}
						i++;
					}
				} // else everything mapped against empty list :)
				
				GraphColouring gce = rooted.get(indexes);
				if(gce==null){
					rooted.put(indexes,gc);
				} else{
					o.addAndCompose(RefinablePartition.getMapping(gce.rfp, gc.rfp));
					TreeSet<Node> orbit = o.getNonTrivialOrbit(next);
					if(orbit!=null && orbit.size()>0){
						for(Node v:visited){
							if(orbit.contains(v)){
								return true;
							}
						}
					}
				}
			}
		}
		
		return false;
	}

	public ArrayList<Integer> getColourIterations(){
		return colourIters;
	}

	public Leaves getLeaves(){
		return leaves;
	}

	public int getTotalColourIterations(){
		int sum = 0;
		for(Integer i:colourIters)
			sum += i;
		return sum;
	}

	//	public TreeSet<Node[]> chooseLowestGraph(){
	////		TreeSet<Node[]> min = null;
	//		
	//		TreeSet<TreeSet<Node[]>> graphs = new TreeSet<TreeSet<Node[]>>(GRAPH_COMP);
	//		
	//		for(GraphColouring gc: complete.values()){
	//			TreeSet<Node[]> lGraph = labelBlankNodes(gc.hg);
	//			
	//			System.err.println("++++++++++++++++++++++++++++++\n+++++++++++++++++++++");
	//			for(Node[] triple:lGraph){
	//				System.err.println(Nodes.toN3(triple));
	//			}
	//			System.err.println("++++++++++++++++++++++++++++++\n+++++++++++++++++++++");
	//			
	//			if(graphs.add(lGraph))
	//				uniquePaths.add(gc.disTree);
	//			
	//			if(min == null){
	//				min = lGraph;
	//				uniquePaths.add(gc.disTree);
	//			} else{
	//				int comp = GRAPH_COMP.compare(min, lGraph);
	//				if(comp>0){
	//					min = lGraph;
	//					uniquePaths.add(gc.disTree);
	//				} else if(comp<0){
	//					uniquePaths.add(gc.disTree);
	//				}
	//			}
	//		}
	//		
	//		return min;
	//		
	//		return graphs.first();
	//	}

	/**
	 * Uses the colourings of a hash graph to relabel the blank
	 * nodes.
	 * 
	 * @param hg
	 * @return A sorted set of triples representing the relabelled bnodes
	 */
	public static TreeSet<Node[]> labelBlankNodes(HashGraph hg){
		TreeSet<Node[]> graph = new TreeSet<Node[]>(TRIPLE_COMP);

		for(Node[] triple: hg.getData()){
			Node[] newTriple = new Node[triple.length];

			for(int i=0; i<triple.length; i++){
				if(triple[i] instanceof BNode){
					newTriple[i] = createBNode(hg.getHash((BNode)triple[i]));
				} else{
					newTriple[i] = triple[i];
				}
			}

			graph.add(newTriple);
		}

		return graph;
	}

	private static BNode createBNode(HashCode hc){
		return new BNode(BNODE_LABEL_PREFIX+hc.toString());
	}

	/**
	 * Runs colouring to fixpoint
	 * @return
	 * @throws HashCollisionException
	 * @throws InterruptedException
	 */
	private int runColouring() throws HashCollisionException, InterruptedException{
		ran = true;

		rfp = new RefinablePartition(hg.getBlankNodeHashes().keySet());
		ArrayList<Node[]> data = hg.getData();

		HashFunction hf = hg.getHashFunction();

		// marks subject
		HashCode plus = hf.hashUnencodedChars("+");

		// marks object
		HashCode minus = hf.hashUnencodedChars("-");

		// avoids repeated code for subject/object
		HashCode[] plusMinus = new HashCode[] { plus, null, minus };
		int[] subjObj = new int[] {2, 1, 0};

		// a reusable tuple for calculating hashes
		ArrayList<HashCode> tup = new ArrayList<HashCode>();

		// NEW: stores set of blank hashes for triples ...
		// unordered combination of hashes causing too many hash collisions
		// in certain schemes :/
		MapArrayList<Node,HashCode> edgeHashes = new  MapArrayList<Node,HashCode>();

		// round
		int r=0;

		// checks done condition
		boolean done;
		
		// previous partition
		// if it doesn't change, we're done
		part = new MapTreeSet<HashCode,Node>(hg.getBlankNodeHashes());
		
		do{
			r++;
			edgeHashes.clear();

			LOG.finer("Running colouring iteration "+r);

			for(Node[] trip:data){

				// avoid looking up the hashcode
				// twice for each order
				HashCode[] hTrip = new HashCode[trip.length];

				for(int i=0; i<trip.length; i++){
					hTrip[i] = hg.getHash(trip[i]);
				}

				for(Integer i : new int[]{0,2}){		
					if(trip[i] instanceof BNode){
						// create a hashcode for the tuple:
						// combineOrdered(s,p,-) or combineOrdered(o,p,+);
						// getHash returns hash for previous round
						tup.add(hTrip[subjObj[i]]);
						tup.add(hTrip[1]);
						tup.add(plusMinus[i]);

						HashCode hcTup = Hashing.combineOrdered(tup);
						edgeHashes.add(trip[i], hcTup);

						tup.clear();
					}
				}
			}

			// will store blank node hashes for the next round
			HashMap<Node,HashCode> nextHashes = new HashMap<Node,HashCode>();


			// NEW: instead of using unordered combination (which caused
			// hash collisions), sort and apply ordered combination instead
			for(Map.Entry<Node,ArrayList<HashCode>> kv:edgeHashes.entrySet()){
				ArrayList<HashCode> hashes = kv.getValue();
				hashes.add(hg.getBlankNodeHashes().get(kv.getKey()));
				Collections.sort(hashes, HashCodeComparator.INSTANCE);
				HashCode hc = Hashing.combineOrdered(hashes);
				nextHashes.put(kv.getKey(), hc);
			}

			
			MapTreeSet<HashCode,Node> newpart = new MapTreeSet<HashCode,Node>(nextHashes);


			// if one of the parts is larger, we've just
			// encountered a hash collision: should be unlikely in
			// practice depending on hashing scheme but can happen for
			// very large, very uniform graphs 
			//
			// we can recover deterministically by hashing the old hash 
			// and the new hash again
			
			// the old partition
			Partition<Node> oldParts = new Partition<Node>(part);

			// the new partition containing collisions
			ArrayList<TreeSet<Node>> col = new ArrayList<TreeSet<Node>>();

			TreeSet<HashCode> oldHashes = new TreeSet<HashCode>(HashCodeComparator.INSTANCE);
			HashMap<HashCode,Integer> oldRank = new HashMap<HashCode,Integer>();
			int i = 0;
			do{
				col.clear();
				
				for(Map.Entry<HashCode,TreeSet<Node>> np:newpart.entrySet()){
					TreeSet<Node> oldPart = oldParts.getPartition(np.getValue().first());
					TreeSet<Node> newPart = np.getValue();
					if(!superseteq(oldPart,newPart)){
						col.add(newPart);
						// rank of old hash will be used to add noise to 
						// differentiate hash
						if(i==0){
							for(Node n:newPart)
								oldHashes.add(hg.getHash(n));
						}
						
						LOG.fine("Found hash collision(s) in round "+r+"! Trying to recover ...");
					}
				}
				
				// this is used to add some deterministic "noise" 
				// to hopefully help differentiate the hashes
				Iterator<HashCode> oldHashIter = oldHashes.iterator();
				int j=1;
				while(oldHashIter.hasNext()){
					oldRank.put(oldHashIter.next(),j*(i+1)*PRIME);
					j++;
				}

				// for all the partitions that grew or otherwise collided
				for(TreeSet<Node> c:col){
					for(Node n:c){
						// mux the old hash and the rank of the old hash
						// with the new hash
						HashCode newhc = nextHashes.get(n);
						HashCode oldhc = hg.getHash(n);
						HashCode oldrc = hg.getHashFunction().hashInt(oldRank.get(oldhc));
						
						ArrayList<HashCode> tupl = new ArrayList<HashCode>();
						tupl.add(newhc);
						tupl.add(oldhc);
						tupl.add(oldrc);
						HashCode comb = Hashing.combineOrdered(tupl);
						LOG.finest("Conflicting node "+n+": new:"+newhc+" old:"+oldhc+" oldr:"+oldrc+" fix:"+comb);
						nextHashes.put(n, comb);
					}
				}

				if(!col.isEmpty()){
					newpart = new MapTreeSet<HashCode,Node>(nextHashes);
					LOG.fine("... recovery "+i+" attempted");
				}
				i++;
			} while(!col.isEmpty() && i<COLLISION_RECOVERY_ATTEMPTS);

			if(!col.isEmpty()){
				System.err.println("Previous partitioning: "+part);
				System.err.println("New partitioning: "+newpart);
				System.err.println("Broken partitions: "+col);
				System.err.println("Blank hash: "+hg.getBlankHash());
				throw new HashCollisionException("Unrecoverable hash collision (cycle?) in round "+r+" of colouring, branch "+path);
			}
			
			// finished if number of partitions doesn't increase 
			// complete if every blank node has been distinguished
			boolean compl = newpart.size() == hg.getBlankNodeHashes().size();
			done = part.size() == newpart.size() || compl;

			// update hashes in the HashGraph
			part = newpart;
			hg.getBlankNodeHashes().putAll(nextHashes);

			//System.err.println(hg.getBlankNodeHashes().size()+" "+part.size()+" "+last);

			if(done){
				rfp.refine(new Partition<Node>(part),hg.getBlankNodeHashes());
			}

			if(compl){
				TreeSet<Node[]> lGraph = labelBlankNodes(hg);
				leaves.add(lGraph, this);
				LOG.fine("Branch "+path+" is a leaf.");
			}

			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
		} while(!done);
		colourIters.add(r);

		return r;
	}
	
	private static <A> boolean superseteq(TreeSet<A> big, TreeSet<A> small){
		if(small.size()>big.size()){
			return false;
		}
		
		// would be faster to do a traversal
		// but I'm too lazy
		
		for(A a:small){
			if(!big.contains(a))
				return false;
		}
		return true;
	}

//	public static void main(String[] args) throws IOException, InterruptedException, HashCollisionException{
//		String file = "test/simple3.nt";
//
//
//		BufferedReader br = new BufferedReader(new FileReader(file));
//		NxParser nxp = new NxParser(br);
//
//		HashFunction hf = Hashing.crc32();
//
//		HashGraph hg = new HashGraph(hf);
//
//		while(nxp.hasNext()){
//			hg.addTriple(nxp.next());
//		}
//
//		Collection<HashGraph> bnps = hg.blankNodePartition();
//
//		TreeMap<TreeSet<Node[]>,Integer> graphs = new TreeMap<TreeSet<Node[]>,Integer>(GraphColouring.GRAPH_COMP);
//		for(HashGraph bnp:bnps){
//			GraphColouring gc = new GraphColouring(bnp);
//			gc.execute();
//
//			TreeSet<Node[]> cg = gc.getCanonicalGraph();
//			Integer count = graphs.get(cg);
//			if(count==null){
//				graphs.put(cg,1);
//			} else{
//				graphs.put(cg,count+1);
//				cg = gc.getCanonicalGraph(count);
//			}
//
//			System.out.println("==============");
//			for(Node[] triple: cg){
//				System.out.println(Nodes.toN3(triple));
//			}
//
//			System.out.println("==============");
//		}
//		br.close();
//
//	}

	public GraphResult call() throws Exception {
		execute();
		return getCanonicalGraph();
	}

	public static class HashCollisionException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public HashCollisionException(){
			super();
		}

		public HashCollisionException(String msg){
			super(msg);
		}
	}

	public static class HashCodeComparator implements Comparator<HashCode>{
		static HashCodeComparator INSTANCE = new HashCodeComparator();


		public int compare(HashCode o1, HashCode o2) {
			return o1.toString().compareTo(o2.toString());
		}
	}
	
	/**
	 * A pair with a hash and a graph.
	 * 
	 * @author Aidan
	 *
	 */
	public static class GraphResult{
		private TreeSet<Node[]> graph;
		private HashCode hash;
		private HashGraph hg;
		
		public GraphResult(TreeSet<Node[]> graph, HashGraph hg, HashCode hash){
			this.graph = graph;
			this.hg = hg;
			this.hash = hash;
		}
		
		public TreeSet<Node[]> getGraph(){
			return graph;
		}
		
		public HashGraph getHashGraph(){
			return hg;
		}
		
		public HashCode getHash(){
			return hash;
		}
	}
}
