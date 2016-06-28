package cl.uchile.dcc.blabel.label;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import cl.uchile.dcc.blabel.label.GraphColouring.GraphResult;
import cl.uchile.dcc.blabel.label.GraphColouring.HashCollisionException;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingResult;
import cl.uchile.dcc.blabel.label.util.HashGraph;

/**
 * Main class wrapping an interface for the underlying classes. Takes care
 * of hashing and partitioning graphs, colouring, and packaging the result
 * along with some additional info.
 * 
 * @author Aidan
 *
 */
public class GraphLabelling implements Callable<GraphLabellingResult> {
	private final Collection<Node[]> data;
	private final GraphLabellingArgs args;
	
	/** 
	 * Canonicalise graph with standard arguments.
	 * @param data
	 */
	public GraphLabelling(Collection<Node[]> data){
		this(data, new GraphLabellingArgs());
	}
	
	/**
	 * Canonicalise graph with custom arguments (e.g., custom hashing)
	 * @param data
	 * @param cla
	 */
	public GraphLabelling(Collection<Node[]> data, GraphLabellingArgs cla){
		this.data = data;
		this.args = cla;
	}
	
	/**
	 * The main execute method. Runs the canonical labelling according to the data
	 * and args provided. Spits out a result with a canonical graph.
	 */
	public GraphLabellingResult call() throws InterruptedException, HashCollisionException{
		// first hash the graph
		HashGraph hg = new HashGraph(args.getHashFunction());
		for(Node[] stmt : data){
			hg.addTriple(stmt);
		}

		// then get out the partitions
		Collection<HashGraph> bnps = hg.blankNodePartition();
		TreeMap<TreeSet<Node[]>,Integer> graphs = new TreeMap<TreeSet<Node[]>,Integer>(GraphColouring.GRAPH_COMP);
		int totalColourIters = 0;
		int leaves = 0;
		
		// this stores the canonical result
		TreeSet<Node[]> fullGraph = new TreeSet<Node[]>(NodeComparator.NC);
		
		ArrayList<HashCode> hashes = new ArrayList<HashCode>();
		
		// the count of bnodes that should be in the output
		int uniqueBnodes = 0;
		
		// more efficient to split the graph
		// per connected blank nodes and merge
		// the results afterwards
		for(HashGraph bnp:bnps){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			// run the colouring for each partition
			GraphColouring gc = new GraphColouring(bnp,args.prune);
			gc.execute();
			
			// get the canonical graph for the partition
			GraphResult ghp = gc.getCanonicalGraph();
			
			// get the mapped data
			TreeSet<Node[]> mapped = ghp.getGraph();
			
			// make sure the graph has not been seen before
			Integer count = graphs.get(mapped);
			if(count==null){
				// if so, increment count
				graphs.put(mapped,1);
				hashes.add(ghp.getHash());
				uniqueBnodes += ghp.getHashGraph().getBlankNodeHashes().size();
			} else{
				// if not, mux the count into the blank node hashes
				// to avoid overwriting
				// a little inefficient to mux again but very rare case ...
				// two (or more) isomorphic bnps in one document
				graphs.put(mapped,count+1);
				if(args.dip){
					ghp = gc.getCanonicalGraph(count+1);
					mapped = ghp.getGraph();
					hashes.add(ghp.getHash());
					uniqueBnodes += ghp.getHashGraph().getBlankNodeHashes().size();
				}
			}
			
			// sum the stats for all partitions to track in result
			totalColourIters += gc.getTotalColourIterations();
			leaves += gc.getLeaves().size();
			
			// collect all triples and hashes
			fullGraph.addAll(mapped);
			
			
			// update the bnodes in the original full graph
			hg.updateBNodeHashes(ghp.getHashGraph().getBlankNodeHashes());
		}
		
		
		// will store a unique graph-level hash
		HashCode ghash = null;
		
		if(args.upg){
			// need to mux in a graph-level unique hash
			
			// get the hash over all ground triples
			HashCode ground = hg.getGroundSubGraphHash();
			// add it with the hashes for all partitions and combine
			hashes.add(ground);
			ghash = Hashing.combineUnordered(hashes);
			
			// mux the combined hashcode
			HashGraph.muxHash(hg, ghash);
						
			// compute final graph
			fullGraph = GraphColouring.labelBlankNodes(hg);
		} else{
			// otherwise we don't need to mux anything else
			
			// need to also add ground triples to result
			// (not included in computed blank node partitions)
			for(Node[] stmt:data){
				boolean ground = true;
				for(Node n:stmt){
					if(n instanceof BNode){
						ground = false;
						break;
					}
				}
				
				if(ground){
					fullGraph.add(stmt);
				}
			}
		}
		
		// just check that we don't have a hash collision
		HashSet<HashCode> checkHashes = new HashSet<HashCode>();
		checkHashes.addAll(hg.getBlankNodeHashes().values());
		if(checkHashes.size()!=uniqueBnodes){
			throw new HashCollisionException("We've produced a hash collision ("+uniqueBnodes+":"+checkHashes.size()+"): "+hg.getBlankNodeHashes());
		}
		
		
		
		
		// fill the data and stats into the result object
		GraphLabellingResult clr = new GraphLabellingResult();
		clr.setGraph(fullGraph);
		clr.setBnodeCount(hg.getBlankNodeHashes().size());
		clr.setPartitionCount(bnps.size());
		clr.setColourIterationCount(totalColourIters);
		clr.setLeafCount(leaves);
		clr.setHashGraph(hg);
		clr.setUniqueGraphHash(ghash);
		
		return clr;
	}
	
	public static class GraphLabellingArgs{
		public static HashFunction DEFAULT_HASHING = Hashing.md5();
		public static boolean DISTINGUISH_ISO_PARTITIONS = true;
		public static boolean DEFAULT_UNIQUE_PER_GRAPH = true;
		public static boolean DEFAULT_PRUNE = true;
		
		// the hashing function to use
		private HashFunction hf = DEFAULT_HASHING;
		
		// distinguish any blank node partitions
		// that are isomorphic
		private boolean dip = DISTINGUISH_ISO_PARTITIONS;
		
		// if false, mux with a partition level hash
		// if true, mux with a graph level hash
		private boolean upg = DEFAULT_UNIQUE_PER_GRAPH;
		
		// if false, do not prune by automorphisms
		// if true, prune by automorphisms
		// NOTE: should be true unless testing!
		private boolean prune = DEFAULT_PRUNE;
		
		public GraphLabellingArgs(){
			
		}
		
		/**
		 * Set the hash function used for the labelling
		 * @param hf
		 */
		public void setHashFunction(HashFunction hf){
			this.hf = hf;
		}
		
		/**
		 * Get the hash function currently set
		 * @return
		 */
		public HashFunction getHashFunction(){
			return hf;
		}
		
		/**
		 * distinguish any blank node partitions
		 * that are isomorphic (otherwise they will
		 * be leaned if they exist)
		 *
		 * for example:
		 * _:a :p _:b .
		 * _:c :p _:d .
		 *
		 * true means these partitions will be distinguished
		 * and two output triples will be given
		 *
		 * false means the same labels will be computed for 
		 * both partitions and only one output triple will be 
		 * given.
		 * 
		 * @return
		 */
		public void setDistinguishIsoPartitions(boolean dip){
			this.dip = dip;
		}
		
		public boolean getDistinguishIsoPartitions(){
			return dip;
		}
		
		/**
		 * Sets whether the blank node labels should be unique
		 * on the level of a given graph or a given partition.
		 * 
		 * If set to false, a unique partition-level hash is muxed with
		 * all blank nodes in each partition.
		 * 
		 * If set to true, a unique graph-level hash is muxed with all
		 * blank nodes in the entire graph.
		 * 
		 * For example take:
		 * _:a :p _:b .
		 * _:b :p _:c .
		 * 
		 * and
		 * 
		 * _:x :p _:y .
		 * _:y :p _:z .
		 * <q> :p <w> .
		 * 
		 * The labelled version of the first graph would be a
		 * subset of the second graph if set to false (since
		 * the partitions are the same). However, if set to true,
		 * then no triples would be shared by both inputs (since the
		 * graphs are different).
		 */
		public void setUniquePerGraph(boolean upg){
			this.upg = upg;
		}
		
		public boolean getUniquePerGraph(){
			return upg;
		}

		public boolean isPrune() {
			return prune;
		}

		public void setPrune(boolean prune) {
			this.prune = prune;
		}
	}
	
	public static class GraphLabellingResult{

		private TreeSet<Node[]> graph;
		private int bnodeCount;
		private int partitionCount;
		private int colourIterationCount;
		private int leafCount;
		private HashGraph hashGraph;
		private HashCode gHash;
		
		private GraphLabellingResult(){
			;
		}
		
		/**
		 * Will only be set if uniquePerGraph was set true
		 * in the arguments. This is a unique graph level hash
		 * used to mux the blank nodes. It serves as an overall
		 * hash for the graph.
		 * 
		 * @param ghash
		 */
		private void setUniqueGraphHash(HashCode ghash) {
			this.gHash = ghash;
		}
		
		/**
		 * Will only be set if uniquePerGraph was set true
		 * in the arguments. This is a unique graph level hash
		 * used to mux the blank nodes. It serves as an overall
		 * hash for the graph.
		 * 
		 * If null, you can try use getHashGraph().getGraphHash()
		 * to compute a new hash. (This will differ from the return
		 * to this call but will equally be a unique hash.)
		 * 
		 * @param ghash
		 */
		public HashCode getUniqueGraphHash(){
			return gHash;
		}

		/**
		 * Get the final canonicalised graph (with blank node labels)
		 * @return
		 */
		public TreeSet<Node[]> getGraph() {
			return graph;
		}
		
		private void setGraph(TreeSet<Node[]> graph) {
			this.graph = graph;
		}
		
		/**
		 * Get the final hash graph with canonical labels
		 * @return
		 */
		public HashGraph getHashGraph() {
			return hashGraph;
		}
		
		private void setHashGraph(HashGraph hashGraph) {
			this.hashGraph = hashGraph;
		}
		
		/**
		 * Get the count of blank nodes in the graph
		 * @return
		 */
		public int getBnodeCount() {
			return bnodeCount;
		}
		
		private void setBnodeCount(int bnodeCount) {
			this.bnodeCount = bnodeCount;
		}
		
		/**
		 * Get the number of blank node partitions in the graph
		 * @return
		 */
		public int getPartitionCount() {
			return partitionCount;
		}
		
		private void setPartitionCount(int partitionCount) {
			this.partitionCount = partitionCount;
		}
		
		/**
		 * Get the total number of colour iterations that needed to be run
		 * (added across all partitions)
		 * @return
		 */
		public int getColourIterationCount() {
			return colourIterationCount;
		}

		private void setColourIterationCount(int colourIterationCount) {
			this.colourIterationCount = colourIterationCount;
		}

		/**
		 * Get the total number of leaves explored in refinement trees
		 * (added across all partitions)
		 * @return
		 */
		public int getLeafCount() {
			return leafCount;
		}

		private void setLeafCount(int leafCount) {
			this.leafCount = leafCount;
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("data/btc-err/label/input_shuffle_0.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/4clique.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/grid.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/square.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/saramandai.nq"));
//		BufferedReader br = new BufferedReader(new FileReader("data/null-test2.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/timeout.nt"));
		NxParser nxp = new NxParser(br);
		

		TreeSet<Node[]> triples = new TreeSet<Node[]>(NodeComparator.NC);
		
		while(nxp.hasNext()){
			Node[] triple = nxp.next();
			triples.add(new Node[]{triple[0],triple[1],triple[2]});
		}
		
		br.close();
		
		GraphLabellingArgs gla = new GraphLabellingArgs();
		gla.setDistinguishIsoPartitions(false);
		GraphLabelling gl = new GraphLabelling(triples,gla);
		
		GraphLabellingResult glr = gl.call();
		
//		for(Node[] leanTriple: glr.leanData){
//			System.err.println(Nodes.toN3(leanTriple));
//		}
		
		System.err.println(glr.bnodeCount);
		System.err.println(glr.colourIterationCount);
		System.err.println(glr.leafCount);
		System.err.println(glr.partitionCount);
		System.err.println(glr.getGraph().size());
		
		for(Node[] out:glr.getGraph()){
			System.err.println(Nodes.toN3(out));
		}
		
		

//		TreeSet<BNode> notEqual = new TreeSet<BNode>();
//		if(glr.coreMap!=null){
//			for(Map.Entry<BNode, Node> e:glr.coreMap.entrySet()){
//				if(!e.getKey().equals(e.getValue())){
//					if(e.getValue() instanceof BNode){
//						notEqual.add((BNode)e.getValue());
//					}
//					System.err.println(e);
//				}
//			}
//			
////			for(Node n:notEqual){
////				if(!glr.coreMap.get(n).equals(n)){
////					System.err.println("Not transitively closed "+n+" "+glr.getCoreMap().get(n));
////				}
////			}
//		}
		
//		for(Node[] a:triples){
//			if(!glr.leanData.contains(a)){
//				System.err.println("Removed "+Nodes.toN3(a));
//			}
//		}
//		
//		for(Node[] a:glr.leanData){
//			if(!triples.contains(a)){
//				System.err.println("Oh oh "+Nodes.toN3(a));
//			}
//		}
	}
}
