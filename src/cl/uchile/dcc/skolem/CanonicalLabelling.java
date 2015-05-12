package cl.uchile.dcc.skolem;

import java.util.Collection;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;

import cl.uchile.dcc.skolem.CanonicalLabelling.CanonicalLabellingResult;
import cl.uchile.dcc.skolem.GraphColouring.HashCollisionException;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Main class wrapping an interface for the underlying classes. Takes care
 * of hashing and partitioning graphs, colouring, and packaging the result
 * along with some additional info.
 * 
 * @author Aidan
 *
 */
public class CanonicalLabelling implements Callable<CanonicalLabellingResult> {
	private final Collection<Node[]> data;
	private final CanonicalLabellingArgs args;
	
	/** 
	 * Canonicalise graph with standard arguments.
	 * @param data
	 */
	public CanonicalLabelling(Collection<Node[]> data){
		this(data, new CanonicalLabellingArgs());
	}
	
	/**
	 * Canonicalise graph with custom arguments (e.g., custom hashing)
	 * @param data
	 * @param cla
	 */
	public CanonicalLabelling(Collection<Node[]> data, CanonicalLabellingArgs cla){
		this.data = data;
		this.args = cla;
	}
	
	/**
	 * The main execute method. Runs the canonical labelling according to the data
	 * and args provided. Spits out a result with a canonical graph.
	 */
	public CanonicalLabellingResult call() throws InterruptedException, HashCollisionException{
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
		
		// more efficient to split the graph
		// per connected blank nodes and merge
		// the results afterwards
		for(HashGraph bnp:bnps){
			// run the colouring for each partition
			GraphColouring gc = new GraphColouring(bnp);
			gc.execute();
			
			// get the canonical graph for the partition
			TreeSet<Node[]> cg = gc.getCanonicalGraph();
			
			// make sure the graph has not been seen before
			Integer count = graphs.get(cg);
			if(count==null){
				// if so, increment count
				graphs.put(cg,1);
			} else{
				// if not, mux the count into the blank node hashes
				// to avoid overwriting
				// a little inefficient to mux again but very rare case ...
				// two (or more) isomorphic bnps in one document
				graphs.put(cg,count+1);
				cg = gc.getCanonicalGraph(count);
			}
			
			// sum the stats for all partitions to track in result
			totalColourIters += gc.getTotalColourIterations();
			leaves += gc.getLeaves().size();
			
			fullGraph.addAll(cg);
			
			hg.updateBNodeHashes(bnp.getBlankNodeHashes());
		}
		
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
		
		// fill the data and stats into the result object
		CanonicalLabellingResult clr = new CanonicalLabellingResult();
		clr.setGraph(fullGraph);
		clr.setBnodeCount(hg.getBlankNodeHashes().size());
		clr.setPartitionCount(bnps.size());
		clr.setColourIterationCount(totalColourIters);
		clr.setLeafCount(leaves);
		clr.setHashGraph(hg);
		
		return clr;
	}
	
	public static class CanonicalLabellingArgs{
		public HashFunction DEFAULT_HASHING = Hashing.sha1();
		
		private HashFunction hf = DEFAULT_HASHING;
		
		public CanonicalLabellingArgs(){
			
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
	}
	
	public static class CanonicalLabellingResult{

		private TreeSet<Node[]> graph;
		private int bnodeCount;
		private int partitionCount;
		private int colourIterationCount;
		private int leafCount;
		private HashGraph hashGraph;
		
		private CanonicalLabellingResult(){
			;
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
}
