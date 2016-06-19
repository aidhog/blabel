package cl.uchile.dcc.blabel.lean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.stats.Count;

import cl.uchile.dcc.blabel.lean.GraphLeaning.GraphLeaningResult;
import cl.uchile.dcc.blabel.lean.util.Bindings;
import cl.uchile.dcc.blabel.lean.util.Edge;
import cl.uchile.dcc.blabel.lean.util.NodeBindCount;
import cl.uchile.dcc.blabel.lean.util.NodeBindCountPair;
import cl.uchile.dcc.blabel.lean.util.NodeEdges;
import cl.uchile.dcc.blabel.lean.util.PatternSelectivityEstimate;
import cl.uchile.dcc.blabel.lean.util.VariableSelectivityEstimate;

public abstract class GraphLeaning implements Callable<GraphLeaningResult>{
	protected final Collection<Node[]> data;

	// data where subject and object are bnodes
	protected final Collection<Node[]> bnodeData;

	// query bnodes (connected, non ground)
	protected Set<BNode> queryBnodes;

	public static final Logger LOG = Logger.getLogger(GraphLeaning.class.getName());

	// maps node to incoming and outgoing
	// ground edges
	protected Map<Node,NodeEdges> nodeToEdges;

	// maps incoming/outgoing ground edge to node
	protected Map<Edge,TreeSet<NodeEdges>> edgeToNodes;

	// all blank nodes
	protected TreeSet<BNode> bnodes;

	// based on ground edges, bnodes can only
	// be mapped to the nodes here
	// (if bnode not in map, no restriction)
	protected Map<BNode,Set<Node>> candidates;

	// P-O-S index
	protected Map<Node,Map<Node,Set<Node>>> posIndex;

	// P-S-O index
	protected Map<Node,Map<Node,Set<Node>>> psoIndex;

	// blank nodes that can only be
	// mapped to themselves based on ground edges
	protected TreeSet<BNode> fixedBnodes;

	// cardinalities for predicates
	protected Count<Node> predCard;

	// number of joins performed [for statistics]
	protected int joins = 0;

	// dummy blank node used for
	// ground edges where value is a
	// blank node
	public static final BNode DUMMY = new BNode("dummy");

	// randomise bindings [ONLY USEFUL FOR BENCHMARKING]
	protected boolean randomiseBindings = false;

	public GraphLeaning(Collection<Node[]> data){
		this(data,false);
	}

	/**
	 * Randomising bindings only useful for benchmarking.
	 * By default, algorithm chooses best bindings to try first.
	 * 
	 * @param data
	 * @param randomiseBindings set to true to randomise order bindings are visited
	 */
	public GraphLeaning(Collection<Node[]> data, boolean randomiseBindings){
		this.data = data;
		this.randomiseBindings = randomiseBindings;
		bnodeData = new ArrayList<Node[]>();
		queryBnodes = new HashSet<BNode>();
		fixedBnodes = new TreeSet<BNode>();
	}

	public GraphLeaningResult call() throws InterruptedException {
		return lean();
	}

	private GraphLeaningResult lean() throws InterruptedException {
		int prevGroundBnodes = 0;

		do{
			prevGroundBnodes = fixedBnodes.size();
			indexGroundEdges();
			findGroundCandidates();
		} while(fixedBnodes.size()!=prevGroundBnodes);

		//fixedBnodes are like constants
		if(fixedBnodes.size() == bnodes.size()){
			// all bnodes have unique
			// ground information ... nothing
			// to lean
			return new GraphLeaningResult(data);
		} 

		indexBNodeGraph();


		Map<BNode,Node> coreMap = null;
		GraphLeaningResult glrConnected = null;
		Collection<Node[]> currentData = this.data;
		if(!bnodeData.isEmpty()){
			// we have connected blank nodes
			ArrayList<Node[]> query = new ArrayList<Node[]>(bnodeData);
			ArrayList<Node[]> orderedQuery = orderPatterns(query);

			glrConnected = getCore(orderedQuery);
			currentData = glrConnected.leanData;
			coreMap = glrConnected.coreMap;

			if(coreMap==null){
				// if there's no proper homomorphism
				// map blank node to themselves
				coreMap = new HashMap<BNode,Node>();
				for(BNode qb:queryBnodes){
					coreMap.put(qb, qb);
				}
			}
		} 

		if(queryBnodes.size()!=bnodes.size()){
			// now we have to take care of unconnected bnodes
			coreMap = getLeanCrossProduct(coreMap);
		}

		TreeSet<Node[]> leanData = mapData(currentData,coreMap);
		GraphLeaningResult glr = new GraphLeaningResult(leanData);
		if(leanData.size()!=data.size()){
			glr.setCoreMap(coreMap);
		}

		if(glrConnected!=null){
			glr.joins = glrConnected.joins;
			glr.depth = glrConnected.depth;
			glr.solCount = glrConnected.solCount;
		}
		return glr;
	}

	protected static TreeSet<Node[]> mapData(Collection<Node[]> data, Map<BNode,Node> map){
		TreeSet<Node[]> leanData = new TreeSet<Node[]>(NodeComparator.NC);
		boolean identity = isIdentityMap(map);
		for(Node[] triple:data){
			Node[] leanTriple = new Node[triple.length];
			System.arraycopy(triple, 0, leanTriple, 0, triple.length);
			if(!identity){
				leanTriple[0] = getMappedNode(triple[0], map);
				leanTriple[2] = getMappedNode(triple[2], map);
			}
			leanData.add(leanTriple);
		}
		return leanData;
	}

	private static boolean isIdentityMap(Map<BNode, Node> map) {
		if(map==null)
			return true;
		for(Map.Entry<BNode, Node> e:map.entrySet()){
			if(!e.getKey().equals(e.getValue()))
				return false;
		}
		return true;
	}

	private HashMap<BNode, Node> getLeanCrossProduct(Map<BNode,Node> coreMap) {
		// non-connected blank nodes
		// results are cross product of candidates
		// need to find the solution with fewest blank nodes
		HashMap<BNode,Node> cross = new HashMap<BNode,Node>();

		// get the set of all bnodes used thus far in
		// the solution
		Set<BNode> bnodesUsed = new HashSet<BNode>();
		if(coreMap!=null){
			cross.putAll(coreMap);
			for(Node n:coreMap.values()){
				if(n instanceof BNode){
					bnodesUsed.add((BNode)n);
				}
			}
		}

		// unconnected blank nodes with multiple candidates
		HashSet<BNode> unconnectedBNodes = new HashSet<BNode>();
		for(BNode b: bnodes){
			if(!queryBnodes.contains(b)){
				if(fixedBnodes.contains(b)){
					bnodesUsed.add(b);
					cross.put(b, b);
				} else {
					unconnectedBNodes.add(b);
				}
			}
		}

		// count the times used
		Count<BNode> bnodes = new Count<BNode>();
		for(BNode b:unconnectedBNodes){
			Set<Node> bcands = candidates.get(b);
			if(bcands==null || bcands.size()<=1){
				LOG.warning(b+" not a non-trivial unconnected blank node, has candidates: "+candidates);
			}
			else {
				for(Node n:bcands){
					if(n instanceof BNode){
						bnodes.add((BNode)n);
					}
				}
			}
		}

		// try to first use a bnode that has been previously used
		// ... otherwise select a most frequent one
		for(BNode b:unconnectedBNodes){
			Set<Node> bcands = candidates.get(b);
			int max = 0;
			BNode maxb = null;
			for(Node n:bcands){
				if(!(n instanceof BNode) || bnodesUsed.contains((BNode)n)){
					// it doesn't matter what constant
					// or previously used blank node
					// we pick, it will get rid of the
					// blank node that maps to it
					cross.put(b, n);
					maxb = null;
					break;
				} else{
					// otherwise find most common
					// blank node and map to that
					BNode bv = (BNode)n;
					int c = bnodes.get(bv);
					if(c>max){
						maxb = bv;
						max = c;
					}
				}
			}
			if(maxb!=null){
				cross.put(b, maxb);
			}
		}

		return cross;
	}

	private static Node getMappedNode(Node n, Map<BNode,Node> homo){
		if(n instanceof BNode){
			Node m = homo.get((BNode)n);
			if(m==null){
				// an unconnected blank node
				// we will deal with this at
				// the end
				return n;
			}
			return m;
		}
		return n;
	}

	private void indexBNodeGraph() {
		predCard = new Count<Node>();
		posIndex = new HashMap<Node,Map<Node,Set<Node>>>();
		psoIndex = new HashMap<Node,Map<Node,Set<Node>>>();

		for(Node[] triple:data){
			if(triple[0] instanceof BNode && !fixedBnodes.contains(triple[0]) && triple[2] instanceof BNode && !fixedBnodes.contains(triple[2])){
				bnodeData.add(triple);
			}

			indexBNodeEdge(new Node[]{triple[1],triple[2],triple[0]},posIndex);
			indexBNodeEdge(new Node[]{triple[1],triple[0],triple[2]},psoIndex);
			predCard.add(triple[1]);
		}
	}

	private void indexGroundEdges(){
		nodeToEdges = new HashMap<Node,NodeEdges>();
		edgeToNodes = new HashMap<Edge,TreeSet<NodeEdges>>();
		
		queryBnodes = new TreeSet<BNode>();

		boolean first = bnodes == null || bnodes.isEmpty();
		if(first)
			bnodes = new TreeSet<BNode>();

		for(Node[] triple:data){
			if(triple.length<3){
				LOG.warning("Not a triple: "+Nodes.toN3(triple));
			} else {
				if(triple[0] instanceof BNode && !fixedBnodes.contains(triple[0])){
					if(first)
						bnodes.add((BNode) triple[0]);
					if(triple[2] instanceof BNode && !fixedBnodes.contains(triple[2])){
						queryBnodes.add((BNode)triple[0]);
						queryBnodes.add((BNode)triple[2]);
					}
				} else{
					Edge edge = new Edge(triple[1],triple[0],false);
					indexGroundEdge(triple[2],edge);
				}
				// will be used to check blank nodes
				// with unique set of predicates
				Edge dummyOut = new Edge(triple[1],DUMMY,false);
				indexGroundEdge(triple[2],dummyOut);


				if(triple[2] instanceof BNode && !fixedBnodes.contains(triple[2])){
					if(first)
						bnodes.add((BNode) triple[2]);
				} else{
					Edge edge = new Edge(triple[1],triple[2],true);
					indexGroundEdge(triple[0],edge);
				}
				Edge dummyIn = new Edge(triple[1],DUMMY,true);
				indexGroundEdge(triple[0],dummyIn);
			}
		}
	}

	/**
	 * Find the nodes a blank node can potentially be mapped
	 * to based on ground edge.
	 * 
	 * If a blank node can only be mapped to itself, add to 
	 * fixedBnodes (i.e., if no other node has edges that
	 * cover the blank node).
	 * 
	 * If a blank node has no ground edge, it will not appear
	 * in candidates nor in fixedBnodes.
	 */
	private void findGroundCandidates(){
		//TODO if there's a large number of blank nodes
		// with same edges, this will be quadratic :(
		// could optimise by grouping on edge-set
		// ... just a little complicated for this time
		// of night
		candidates = new HashMap<BNode,Set<Node>>();
		for(BNode bnode:bnodes){
			if(!fixedBnodes.contains(bnode)){
				NodeEdges edges = nodeToEdges.get(bnode);
				if(edges!=null){
					// find most selective edge
					TreeSet<NodeEdges> min = null;

					for(Edge e: edges.getEdges()){
						TreeSet<NodeEdges> ne = edgeToNodes.get(e);
						if(min == null || ne.size() < min.size()){
							min = ne;
						}
						if(ne.size()==1){
							// node can only be mapped to itself
							break;
						}
					}

					// nodes other than itself to which
					// the bnode can be mapped
					TreeSet<Node> cans = new TreeSet<Node>();
					if(min.size()!=1){
						// check each to make sure
						// blank node has subset of edges
						for(NodeEdges ne: min){
							if(!ne.getNode().equals(bnode)){
								if(ne.getEdges().containsAll(edges.getEdges())){
									cans.add(ne.getNode());
								}
							}
						}
					} 

					if(cans.isEmpty()){
						fixedBnodes.add(bnode);
					} 

					// add node itself back in
					cans.add(bnode);
					candidates.put(bnode,cans);
				}
			}
		}
	}

	protected Bindings getBindings(Node[] current, HashMap<BNode,Node> partialSol) throws InterruptedException{
		return getBindings(current, partialSol, null);
	}

	protected Bindings getBindings(Node[] current, HashMap<BNode,Node> partialSol, Count<Node> timesBound) throws InterruptedException{
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		Node[] bound = new Node[current.length];
		System.arraycopy(current, 0, bound, 0, bound.length);

		bound[0] = partialSol.get(current[0]);
		bound[2] = partialSol.get(current[2]);

		// these are the new blank nodes variables that
		// will be bound
		ArrayList<BNode> mask = new ArrayList<BNode>();

		// bindings in same order as the mask
		ArrayList<Node[]> bindings = new ArrayList<Node[]>();

		if(bound[0]==null && bound[2]==null){ // variable bnode in subject and object
			boolean sameVars = current[0].equals(current[2]);

			mask.add((BNode)current[0]);
			if(!sameVars){
				mask.add((BNode)current[2]);
			}

			ArrayList<NodeBindCountPair> orderedBindings = new ArrayList<NodeBindCountPair>(); 

			Map<Node,Set<Node>> osEdges = posIndex.get(current[1]);
			for(Node o : osEdges.keySet()){
				// check that o is compatible with ground data
				if(compatibleWithGroundEdges(current[2],o)){
					// number of times o value is already bound
					int boundO = getIntegerCount(o,timesBound);
					NodeBindCount nbco = new NodeBindCount(o,boundO,o.equals(current[2]));

					for(Node s : osEdges.get(o)){
						// if o and s blank nodes are the same, binding
						// must be the same
						if(!sameVars || s.equals(o)){
							// check that s is compatible with ground data
							if(compatibleWithGroundEdges(current[0],s)){
								// number of times s value is already bound
								int boundS = getIntegerCount(s,timesBound);
								NodeBindCount nbcs = new NodeBindCount(s,boundS,s.equals(current[0]));

								orderedBindings.add(new NodeBindCountPair(nbcs,nbco));
							}
						}
					}	
				}
			}

			if(!orderedBindings.isEmpty()){
				// will select mappings with ground terms
				// and then terms most frequently bound
				// and then non-self matches
				Collections.sort(orderedBindings);

				for(NodeBindCountPair nbcp : orderedBindings){
					if(sameVars){
						bindings.add(new Node[]{nbcp.getSubject().getNode()});
					} else{
						bindings.add(new Node[]{nbcp.getSubject().getNode(),nbcp.getObject().getNode()});
					}
				}
			} else{
				// dead end binding
				return new Bindings(mask,null);
			}
		} else if(bound[0]!=null && bound[2]!=null){
			if(!getVals(bound[1],bound[0],psoIndex).contains(bound[2])){
				// dead end binding
				return new Bindings(mask,null);
			}
			bindings.add(new Node[]{});
		} else {
			int b = (bound[0]!=null) ? 0 : 2;
			int ub = (bound[0]!=null) ? 2 : 0;
			Map<Node,Map<Node,Set<Node>>> index = (bound[0]!=null) ? psoIndex : posIndex;

			mask.add((BNode)current[ub]);

			Set<Node> vals = getVals(bound[1],bound[b],index);

			ArrayList<NodeBindCount> newBindings = new ArrayList<NodeBindCount>();
			if(vals!=null){
				for(Node v:vals){
					// check that v is compatible with ground data
					if(compatibleWithGroundEdges(current[ub],v)){
						// number of times value is already bound
						int boundV = getIntegerCount(v,timesBound);
						NodeBindCount nbc = new NodeBindCount(v,boundV,v.equals(current[ub]));
						newBindings.add(nbc);
					}
				}
			}

			if(!newBindings.isEmpty()){
				Collections.sort(newBindings);

				for(NodeBindCount nbc:newBindings){
					bindings.add(new Node[]{nbc.getNode()});
				}
			} else {
				// dead end binding
				return new Bindings(mask,null);
			}
		}

		// only for benchmark purposes to see
		// benefit of ordering bindings in DFS
		// SHOULD NOT BE SET OTHERWISE
		if(randomiseBindings){
			Collections.shuffle(bindings);
		}

		return new Bindings(mask,bindings);
	}

	private void indexGroundEdge(Node n, Edge e){
		NodeEdges edges = nodeToEdges.get(n);

		if(edges==null){
			edges = new NodeEdges(n);
			nodeToEdges.put(n, edges);
		}

		edges.addEdge(e);

		TreeSet<NodeEdges> nodes = edgeToNodes.get(e);
		if(nodes==null){
			nodes = new TreeSet<NodeEdges>();
			edgeToNodes.put(e, nodes);
		}
		nodes.add(edges);
	}

	/**
	 * 
	 * @param edge is POS or PSO order
	 * @param map to index in
	 */
	private boolean indexBNodeEdge(Node[] edge, Map<Node,Map<Node,Set<Node>>> map){
		Map<Node,Set<Node>> edges = map.get(edge[0]);
		if(edges==null){
			edges = new HashMap<Node,Set<Node>>();
			map.put(edge[0], edges);
		}

		Set<Node> vals = edges.get(edge[1]);
		if(vals==null){
			vals = new HashSet<Node>();
			edges.put(edge[1],vals);
		}

		return vals.add(edge[2]);
	}

	private ArrayList<Node[]> orderPatterns(ArrayList<Node[]> patterns){
		// order patterns by selectivity
		ArrayList<PatternSelectivityEstimate> ordered = new ArrayList<PatternSelectivityEstimate>();

		// keep track of variable selectivities as well
		Map<BNode,VariableSelectivityEstimate> vestimates = new HashMap<BNode,VariableSelectivityEstimate>(); 

		for(Node[] pattern:patterns){
			int pcard = predCard.get(pattern[1]);

			int scard = getSelectivityEstimate((BNode)pattern[0],pattern[1],psoIndex,vestimates);
			int ocard = getSelectivityEstimate((BNode)pattern[2],pattern[1],psoIndex,vestimates);

			ordered.add(new PatternSelectivityEstimate(pattern,scard,pcard,ocard));
		}
		Collections.sort(ordered);

		// index patterns by variables
		HashMap<BNode,TreeSet<PatternSelectivityEstimate>> varToPattern = new HashMap<BNode,TreeSet<PatternSelectivityEstimate>>();
		for(PatternSelectivityEstimate pse: ordered){
			mapVarToPattern((BNode)pse.getPattern()[0],pse,varToPattern);
			mapVarToPattern((BNode)pse.getPattern()[2],pse,varToPattern);
		}

		// select patterns grouped by variables first,
		// ordered by selectivity second
		ArrayList<Node[]> rawOrdered = new ArrayList<Node[]>();
		TreeSet<VariableSelectivityEstimate> queue = new TreeSet<VariableSelectivityEstimate>();
		TreeSet<Node[]> done = new TreeSet<Node[]>(NodeComparator.NC);
		HashSet<Node> varsDone = new HashSet<Node>();

		while(rawOrdered.size()<patterns.size()){
			if(queue.isEmpty()){
				// get the most selective pattern
				// not yet added
				PatternSelectivityEstimate first = null;
				boolean added = false;
				do {  
					first = ordered.remove(0);
					if(added = done.add(first.getPattern())){
						rawOrdered.add(first.getPattern());
						addNewBNodesFromPattern(first.getPattern(),vestimates,varsDone,queue);
					}
				} while(!added);
			} else{
				// get the most selective variable
				// already seen
				VariableSelectivityEstimate vse = queue.pollFirst();
				TreeSet<PatternSelectivityEstimate> pses = varToPattern.get(vse.getVariable());
				for(PatternSelectivityEstimate pse:pses){
					// add all the unseen patterns in order
					// of selectivity for that variable
					if(done.add(pse.getPattern())){
						rawOrdered.add(pse.getPattern());
						addNewBNodesFromPattern(pse.getPattern(),vestimates,varsDone,queue);
					}
				}
			}
		}

		return rawOrdered;
	}

	private void addNewBNodesFromPattern(Node[] pattern, Map<BNode, VariableSelectivityEstimate> vestimates,
			HashSet<Node> varsDone, TreeSet<VariableSelectivityEstimate> queue) {
		addNewBNodeFromPattern(pattern[0],vestimates,varsDone,queue);
		addNewBNodeFromPattern(pattern[2],vestimates,varsDone,queue);
	}


	private void addNewBNodeFromPattern(Node node, Map<BNode, VariableSelectivityEstimate> vestimates, HashSet<Node> varsDone,
			TreeSet<VariableSelectivityEstimate> queue) {
		if(varsDone.add(node)){
			queue.add(vestimates.get(node));
		}
	}

	private int getSelectivityEstimate(BNode var, Node pred, Map<Node, Map<Node, Set<Node>>> index,
			Map<BNode, VariableSelectivityEstimate> vestimates) {
		int card = index.get(pred).size();
		Set<Node> cands = candidates.get(var);
		if(cands != null){
			card = Math.min(card,cands.size());
		}

		VariableSelectivityEstimate vse = vestimates.get(var);
		if(vse==null){
			vse = new VariableSelectivityEstimate(var,card);
			vestimates.put(var, vse);
		} else{
			vse.updateCardinality(card);
		}
		return vse.getCard();
	}

	private static boolean mapVarToPattern(BNode node, PatternSelectivityEstimate pse,
			HashMap<BNode, TreeSet<PatternSelectivityEstimate>> varToPattern) {
		TreeSet<PatternSelectivityEstimate> ts = varToPattern.get(node);
		if(ts == null){
			ts = new TreeSet<PatternSelectivityEstimate>();
			varToPattern.put(node, ts);
		}
		return ts.add(pse);
	}

	/**
	 * Gets the homomorphism with the fewest blank nodes guaranteed
	 * @param query
	 * @return
	 * @throws Exception 
	 */
	protected abstract GraphLeaningResult getCore(ArrayList<Node[]> query)  throws InterruptedException;

	public static HashSet<BNode> getBNodeBindings(HashMap<BNode, Node> partialSol) {
		HashSet<BNode> bnodes = new HashSet<BNode>();
		for(Node n:partialSol.values()){
			if(n instanceof BNode){
				bnodes.add((BNode)n);
			}
		}
		return bnodes;
	}

	protected boolean compatibleWithGroundEdges(Node var, Node bind){
		if(var == bind) return true;
		Set<Node> cands = candidates.get(var);
		if(cands==null || cands.contains(bind)){
			return true;
		}
		return false;
	}

	public static int getIntegerCount(Node n, Count<Node> counts){
		if(counts==null)
			return 0;
		Integer co = counts.get(n);
		return (co==null) ? 0 : co;
	}

	public static <E> E getVals(Node pred, Node node, Map<Node, Map<Node,E>> index) {
		Map<Node,E> edges = index.get(pred);
		if(edges!=null){
			return edges.get(node);
		}
		return null;
	}

	protected static String toN3(Collection<Node[]> data){
		StringBuilder sb = new StringBuilder();
		for(Node[] t:data){
			sb.append(Nodes.toN3(t)+"\n");
		}
		return sb.toString();
	}

	public static class GraphLeaningResult{
		protected Collection<Node[]> leanData;
		protected Map<BNode,Node> coreMap;
		protected int joins;
		protected int depth;
		protected int solCount;

		GraphLeaningResult(Collection<Node[]> leanData){
			this.leanData = leanData;
		}

		public Collection<Node[]> getLeanData(){
			return leanData;
		}

		public String toString(){
			return toN3(leanData);
		}

		/**
		 * 
		 * @return core solution mapping, or null if nothing changes (only automorphisms found)
		 */
		public Map<BNode, Node> getCoreMap() {
			return coreMap;
		}

		public void setCoreMap(Map<BNode, Node> coreMap) {
			this.coreMap = coreMap;
		}

		public int getJoins() {
			return joins;
		}

		public void setJoins(int joins) {
			this.joins = joins;
		}

		public int getDepth() {
			return depth;
		}

		public void setDepth(int depth) {
			this.depth = depth;
		}

		public int getSolutionCount() {
			return solCount;
		}

		public void setSolutionCount(int solCount) {
			this.solCount = solCount;
		}
	}
}
