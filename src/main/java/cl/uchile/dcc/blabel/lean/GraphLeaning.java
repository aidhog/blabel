package cl.uchile.dcc.blabel.lean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
	
	// filtering trivial non-lean blank nodes
	protected TreeSet<Node[]> filteredData;

	// trivial non-lean bnode map
	protected HashMap<BNode,Node> nlbnodeMap;
	
	// data where subject and object are bnodes
	protected Collection<Node[]> bnodeData;

	// query bnodes (connected, non ground)
	protected Set<BNode> queryBnodes;

	public static final Logger LOG = Logger.getLogger(GraphLeaning.class.getName());

	// maps node to all incoming and outgoing edges
	protected Map<Node,NodeEdges> nodeToAllEdges;

	// maps incoming/outgoing edges to node
	protected Map<Edge,TreeSet<NodeEdges>> anyEdgeToNodes;
	
	// maps node to incoming and outgoing
	// ground edges
	protected Map<Node,NodeEdges> nodeToGroundEdges;

	// maps incoming/outgoing ground edge to node
	protected Map<Edge,TreeSet<NodeEdges>> groundEdgeToNodes;

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
	protected long joins = 0;

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
		// first we recursively remove all blank nodes where there
		// is another term with a superset of (exact) edges
		Collection<Node[]> inputData = data;
		nlbnodeMap = new HashMap<BNode,Node>();
		int prevNlbnodes = 0;
		do{
			// previous number of non lean bnodes
			prevNlbnodes = nlbnodeMap.size();
			
			// index exact edges
			indexAllEdges(inputData);
			
			if(bnodes.size()==0){
				// if there are no blank nodes (remaining)
				// we can return the lean graph
				GraphLeaningResult glr = new GraphLeaningResult(inputData);
				glr.setCoreMap(nlbnodeMap);
				return glr;
			}
			
			// removes blank nodes whose edges are a subset
			// (or equal, if multiple) another term
			filterTrivialNonLeanBnodes(inputData);
			inputData = filteredData;
		} while(prevNlbnodes!=nlbnodeMap.size());
		
		// afterwards, for the remaining blank nodes
		// we will build a set of candidates based on
		// ground information; at the same time, we
		// will fix blank nodes with unique ground
		// information.
		int prevGroundBnodes = 0;
		do{
			// we will do this iteratively since fixing
			// a blank node may lead to another blank node
			// being fixed
			prevGroundBnodes = fixedBnodes.size();
			indexGroundEdges(filteredData);
			findGroundCandidates();
		} while(fixedBnodes.size()!=prevGroundBnodes);
		
		// this will be the witness homomorphism/mapping
		HashMap<BNode,Node> coreMap = new HashMap<BNode,Node>();
		if(!fixedBnodes.isEmpty()){
			// we can create a map from fixed bnodes to 
			// themselves to start with
			for(BNode b:fixedBnodes){
				coreMap.put(b, b);
			}
			
			// we ignore the non-lean bnodes for the moment
			// since we will work with the filtered data
		}
				
		//fixedBnodes are like constants
		if(fixedBnodes.size() == bnodes.size()){
			// if all bnodes are fixed due to having unique
			// ground information ... nothing to lean
			
			GraphLeaningResult glr = new GraphLeaningResult(filteredData);
			glr.setCoreMap(coreMap);
			if(!nlbnodeMap.isEmpty()){
				// just add the non-lean blank nodes to the mapping
				// if any and map bnodes to themselves
				glr.coreMap.putAll(nlbnodeMap);
				
			}
			return glr;
		} 
		
		// all we are left to take care of now are the
		// remaining connected blank nodes that are
		// not fixed
		indexBNodeGraph();

		GraphLeaningResult glr = null; 
//		glr.setCoreMap(coreMap);
		
		// we have connected blank nodes
		if(!bnodeData.isEmpty()){
			// create a query (triples with connected non-fixed bnodes
			// created by indexBnodeGraph())
			ArrayList<Node[]> query = new ArrayList<Node[]>(bnodeData);
			// and order by selectivity estimates
			ArrayList<Node[]> orderedQuery = orderPatterns(query);

			// now find a solution (e.g., using BFS or DFS) for
			// the query against the filtered graph
			GraphLeaningResult glrConnected = getCore(orderedQuery,coreMap);

			if(glrConnected.coreMap==null){
				// if there's no proper homomorphism
				// filtered input is lean
				glr = new GraphLeaningResult(filteredData);
				
				// map blank nodes to themselves
				for(BNode qb:queryBnodes){
					coreMap.put(qb, qb);
				}
				glr.setCoreMap(coreMap);
			} else{
				// otherwise we found a proper homomoprhism
				// so let's map the data and set the mapping
				glr = new GraphLeaningResult(glrConnected.leanData);
				glr.setCoreMap(glrConnected.coreMap);
			}

			// add some other statistics
			if(glrConnected!=null){
				glr.joins = glrConnected.joins;
				glr.depth = glrConnected.depth;
				glr.solCount = glrConnected.solCount;
			}
		} else{
			// actually we should have processed all unconnected
			// bnodes by now ... so something is wrong
			LOG.warning("No unconnected bnodes but some bnodes not fixed or non-lean?");
		}
		
		// we could ignore the trivial non-lean bnodes until now,
		// but let's add them back in at the end
		// to complete the witness mapping
		if(!nlbnodeMap.isEmpty()){
			if(glr.coreMap==null)
				glr.coreMap = nlbnodeMap;
			else{
				glr.coreMap.putAll(nlbnodeMap);
				// may need to compute the transitive closure again
				// since if a maps to b in non-lean bnodes and b
				// maps to c in connected homomorphism, we would like
				// to map a to c
				glr.coreMap = transitiveClosure(glr.coreMap);
			}
		}
		
		return glr;
	}

	public static TreeSet<Node[]> mapData(Collection<Node[]> data, Map<BNode,Node> map) throws InterruptedException{
		TreeSet<Node[]> leanData = new TreeSet<Node[]>(NodeComparator.NC);
		boolean identity = isIdentityMap(map);
		for(Node[] triple:data){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
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

	private void indexBNodeGraph() throws InterruptedException {
		predCard = new Count<Node>();
		posIndex = new HashMap<Node,Map<Node,Set<Node>>>();
		psoIndex = new HashMap<Node,Map<Node,Set<Node>>>();
		queryBnodes = new TreeSet<BNode>();
		
		for(Node[] triple:filteredData){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			if(triple[0] instanceof BNode && !fixedBnodes.contains(triple[0]) && triple[2] instanceof BNode && !fixedBnodes.contains(triple[2])){
				bnodeData.add(triple);
				queryBnodes.add((BNode)triple[0]);
				queryBnodes.add((BNode)triple[2]);
			}

			indexBNodeEdge(new Node[]{triple[1],triple[2],triple[0]},posIndex);
			indexBNodeEdge(new Node[]{triple[1],triple[0],triple[2]},psoIndex);
			predCard.add(triple[1]);
		}
	}
	
	private void filterTrivialNonLeanBnodes(Collection<Node[]> data) throws InterruptedException{
		// this stores blank nodes that have the same edge set
		// only necessary to compute the mapping
		HashMap<BNode,TreeSet<BNode>> partition = new HashMap<BNode,TreeSet<BNode>>();
		
		// we will map bnodes to the nodes that witness
		// their non-leanness
		HashMap<BNode,Node> map = new HashMap<BNode,Node>();
		
		for(BNode bnode:bnodes){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			TreeSet<BNode> part = partition.get(bnode);
			if(part!=null){
				// we already found a blank node with an
				// equal set of edges
				continue;
			}
			
			NodeEdges edges = nodeToAllEdges.get(bnode);
			
			if(edges!=null){
				// find most selective edge
				TreeSet<NodeEdges> min = null;

				for(Edge e: edges.getEdges()){
					TreeSet<NodeEdges> ne = anyEdgeToNodes.get(e);
					if(min == null || ne.size() < min.size()){
						min = ne;
					}
					if(ne.size()==1){
						// node can only be mapped to itself
						break;
					}
				}

				// nodes other than itself that has a superset of edges
				if(min.size()!=1){
					// check each to make sure
					// blank node has subset of edges
					for(NodeEdges ne: min){
						if(!ne.getNode().equals(bnode)){
							ArrayList<TreeSet<Edge>> diff = diff(edges.getEdges(),ne.getEdges());
							if(diff.get(0).isEmpty()){
								// has a subset of edges: ne covers edges
								if(diff.get(1).isEmpty()){
									// the sets are equal: ne equals edges
									// if the node in question is
									// IRI or seen blank node,
									// current blank node is redundant
									if(!(ne.getNode() instanceof BNode)){
										map.put(bnode, ne.getNode());
										break;
									} else {
										// node is a blank node, we add it
										// to the partition of bnodes with
										// equal edges
										part = new TreeSet<BNode>();
										part.add(bnode);
										part.add((BNode)ne.getNode());
										partition.put(bnode,part);
										partition.put((BNode)ne.getNode(),part);
									}
										
								} else {
									// ne proper superset of edges
									// current blank node is redundant
									map.put(bnode, ne.getNode());
									break;
								}
							}
						}
					}
				} 
			}
		}
		
		// for all blank nodes with same edges
		// chose first blank node to remain
		// but only if not in nonLeanBnodes
		// (the first will also be the first
		// encountered above ... we have the same
		// iteration order ... so we know it 
		// will be mapped if possible)
		for(TreeSet<BNode> parts:partition.values()){
			BNode first = parts.pollFirst();
			Node mapped = map.get(first);
			if(mapped==null){
				mapped = first;
			}
			
			for(BNode rest : parts){
				map.put(rest,mapped);
			}
		}
		
		// now we compute the transitive closure of the
		// map to make sure we map to the final value
		// and not one that is non lean
		//
		// note we should not have cycles since all bnodes
		// with equal edges have been mapped to a single value
		// (... if we have cycles, this will loop forever)
		if(!map.isEmpty()){
			nlbnodeMap.putAll(map);
			nlbnodeMap = transitiveClosure(nlbnodeMap);
		}
		
		bnodes.removeAll(nlbnodeMap.keySet());
		filteredData = new TreeSet<Node[]>(NodeComparator.NC);
		for(Node[] triple:data){
			if((!(triple[0] instanceof BNode) || !nlbnodeMap.containsKey(triple[0])) && (!(triple[2] instanceof BNode) || !nlbnodeMap.containsKey(triple[2]))){
				filteredData.add(triple);
			}
		}
	}
	
	/**
	 * If a maps to b and b maps to c, output will map a to c.
	 * 
	 * NOTE: ASSUMES NO CYCLES, OTHERWISE THIS WILL NOT TERMINATE CORRECTLY!!!!
	 * 
	 * @param map
	 * @return
	 * @throws InterruptedException 
	 */
	private static HashMap<BNode,Node> transitiveClosure(HashMap<BNode,Node> map) throws InterruptedException{
		boolean changed;
		int iters = 0;
		do{
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			changed = false;
			HashMap<BNode,Node> nextMap = new HashMap<BNode,Node>();
			
			for(Map.Entry<BNode, Node> m: map.entrySet()){
				if(m.getValue() instanceof BNode){
					BNode v = (BNode) m.getValue();
					Node mv = map.get(v);
					
					if(mv!=null && !mv.equals(m.getValue())){
						nextMap.put(m.getKey(), mv);
						changed = true;
						continue;
					}
				}
				nextMap.put(m.getKey(),m.getValue());
			}
			map = nextMap;
			
			// without cycles, this should never happen
			iters++;
			if(iters>map.size()){
				LOG.warning("Found a map with cycles!!! "+map);
				return map;
			}
		} while(changed);
		
		return map;
	}
	
	private void indexAllEdges(Collection<Node[]> data) throws InterruptedException{
		nodeToAllEdges = new HashMap<Node,NodeEdges>();
		anyEdgeToNodes = new HashMap<Edge,TreeSet<NodeEdges>>();
		
		bnodes = new TreeSet<BNode>();

		for(Node[] triple:data){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			if(triple.length<3){
				LOG.warning("Not a triple: "+Nodes.toN3(triple));
			} else {
				if(triple[0] instanceof BNode){
					bnodes.add((BNode) triple[0]);
				} 
				
				Edge inEdge = new Edge(triple[1],triple[0],false);
				indexEdge(triple[2],inEdge,nodeToAllEdges,anyEdgeToNodes);

				if(triple[2] instanceof BNode){
					bnodes.add((BNode) triple[2]);
				}
				
				Edge outEdge = new Edge(triple[1],triple[2],true);
				indexEdge(triple[0],outEdge,nodeToAllEdges,anyEdgeToNodes);
			}
		}
	}

	private void indexGroundEdges(Collection<Node[]> data) throws InterruptedException{
		nodeToGroundEdges = new HashMap<Node,NodeEdges>();
		groundEdgeToNodes = new HashMap<Edge,TreeSet<NodeEdges>>();
		
		for(Node[] triple: data){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			if(triple.length<3){
				LOG.warning("Not a triple: "+Nodes.toN3(triple));
			} else {
				if(!(triple[0] instanceof BNode && !fixedBnodes.contains(triple[0]))){
					// term triple[0] is ground
					Edge edge = new Edge(triple[1],triple[0],false);
					indexEdge(triple[2],edge,nodeToGroundEdges,groundEdgeToNodes);
				}
				// will be used to check blank nodes
				// with unique set of predicates
				Edge dummyOut = new Edge(triple[1],DUMMY,false);
				indexEdge(triple[2],dummyOut,nodeToGroundEdges,groundEdgeToNodes);


				if(!(triple[2] instanceof BNode && !fixedBnodes.contains(triple[2]))){
					// term triple[2] is ground
					Edge edge = new Edge(triple[1],triple[2],true);
					indexEdge(triple[0],edge,nodeToGroundEdges,groundEdgeToNodes);
				}
				Edge dummyIn = new Edge(triple[1],DUMMY,true);
				indexEdge(triple[0],dummyIn,nodeToGroundEdges,groundEdgeToNodes);
			}
		}
	}
	
	private void findGroundCandidates() throws InterruptedException{
		candidates = new HashMap<BNode,Set<Node>>();
		
		for(BNode bnode:bnodes){
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			if(!fixedBnodes.contains(bnode)){
				NodeEdges edges = nodeToGroundEdges.get(bnode);
				if(edges!=null){
					// find most selective edge
					TreeSet<NodeEdges> min = null;

					for(Edge e: edges.getEdges()){
						TreeSet<NodeEdges> ne = groundEdgeToNodes.get(e);
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
								ArrayList<TreeSet<Edge>> diff = diff(edges.getEdges(),ne.getEdges());
								if(diff.get(0).isEmpty()){
									// all edges contained in ne
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
	
	/**
	 * Will return a list of two sets where the first set has the elements
	 * of a not in b, and the second set has the elements of b not in a.
	 * 
	 * Assumes sorting is equal (and no dupes).
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static <E extends Comparable<? super E>> ArrayList<TreeSet<E>> diff(TreeSet<E> a, TreeSet<E> b){
		ArrayList<TreeSet<E>> diff = new ArrayList<TreeSet<E>>();
		diff.add(new TreeSet<E>());
		diff.add(new TreeSet<E>());
		
		Iterator<E> aiter = a.iterator();
		Iterator<E> biter = b.iterator();
		
		E anext = null;
		E bnext = null;
		
		if(aiter.hasNext() && biter.hasNext()){
			anext = aiter.next();
			bnext = biter.next();

			while(aiter.hasNext() && biter.hasNext()){
				int comp = anext.compareTo(bnext);
				
				if(comp<0){
					diff.get(0).add(anext);
					anext = aiter.next();
				} else if(comp>0){
					diff.get(1).add(bnext);
					bnext = biter.next();
				} else{
					anext = aiter.next();
					bnext = biter.next();
				}
			}
			
			int comp = anext.compareTo(bnext);
			
			// we are at the end of one set
			if(comp<0){
				// if a is less, we can add a
				diff.get(0).add(anext);
				
				// we may be able to add b if a
				// is finished or a does not contain b later
				if(!aiter.hasNext() || !a.contains(bnext))
					diff.get(1).add(bnext);
			} else if(comp>0){
				// likewise swapping b and a
				diff.get(1).add(bnext);
				
				if(!biter.hasNext() || !b.contains(anext))
					diff.get(0).add(anext);
			}
		}
		
		// we need to add all the elements of the
		// remaining set
		if(aiter.hasNext()){
			while(aiter.hasNext()){
				diff.get(0).add(aiter.next());
			}
			// more efficient to try remove once
			// than do linear checks
			if(bnext!=null)
				diff.get(0).remove(bnext);
		}
		
		if(biter.hasNext()){
			while(biter.hasNext()){
				diff.get(1).add(biter.next());
			}
			// more efficient to try remove once
			// than do linear checks
			if(anext!=null)
				diff.get(1).remove(anext);
		}
		
		return diff;
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

	private static void indexEdge(Node n, Edge e, Map<Node, NodeEdges> nodeToGroundEdges, Map<Edge, TreeSet<NodeEdges>> groundEdgeToNodes){
		NodeEdges edges = nodeToGroundEdges.get(n);

		if(edges==null){
			edges = new NodeEdges(n);
			nodeToGroundEdges.put(n, edges);
		}

		edges.addEdge(e);

		TreeSet<NodeEdges> nodes = groundEdgeToNodes.get(e);
		if(nodes==null){
			nodes = new TreeSet<NodeEdges>();
			groundEdgeToNodes.put(e, nodes);
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
	protected abstract GraphLeaningResult getCore(ArrayList<Node[]> query, HashMap<BNode, Node> coreMap)  throws InterruptedException;

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
		protected HashMap<BNode,Node> coreMap;
		protected long joins;
		protected int depth;
		protected long solCount;

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

		public void setCoreMap(HashMap<BNode, Node> coreMap) {
			this.coreMap = coreMap;
		}

		public long getJoins() {
			return joins;
		}

		public void setJoins(long joins) {
			this.joins = joins;
		}

		public int getDepth() {
			return depth;
		}

		public void setDepth(int depth) {
			this.depth = depth;
		}

		public long getSolutionCount() {
			return solCount;
		}

		public void setSolutionCount(long solCount) {
			this.solCount = solCount;
		}
	}
}
