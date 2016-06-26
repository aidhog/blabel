package cl.uchile.dcc.blabel.lean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;

import cl.uchile.dcc.blabel.label.util.Orbits;
import cl.uchile.dcc.blabel.lean.util.Bindings;

public class DFSGraphLeaning extends GraphLeaning{
	
	public static final boolean DEFAULT_PRUNE = true;
	
	public static final Logger LOG = Logger.getLogger(DFSGraphLeaning.class.getName());
	
	// whether or not to try prune by automorphisms found
	private boolean prune = DEFAULT_PRUNE;
	
	private ArrayList<ArrayList<Node>> automorphisms = null;
	private ArrayList<BNode> automorphismMask = null;
	
	public DFSGraphLeaning(Collection<Node[]> data){
		super(data);
	}
	
	public DFSGraphLeaning(Collection<Node[]> data, boolean randomiseBindings){
		super(data,randomiseBindings);
	}
	
	public DFSGraphLeaning(Collection<Node[]> data, boolean randomiseBindings, boolean prune){
		super(data,randomiseBindings);
		this.prune = prune;
	}
	
	/**
	 * Gets the homomorphism with the fewest blank nodes guaranteed
	 * @param query
	 * @return
	 * @throws InterruptedException 
	 * @throws Exception 
	 */
	protected GraphLeaningResult getCore(ArrayList<Node[]> query, HashMap<BNode,Node> initialMap) throws InterruptedException {
		HashMap<BNode,Node> homo = getHomomorphism(query, initialMap);
		GraphLeaningResult glr = null;
		if(homo==null){
			// graph is lean, we only have automorphisms
			glr = new GraphLeaningResult(filteredData);
			glr.depth = 1;
		} else{
			//TODO we could probably be more efficient here
			// reusing some work and doing some better checks
			
			// map the data per the homomorphism we found
			// and recurse on everything
			TreeSet<Node[]> leanerData = mapData(filteredData,homo);
			DFSGraphLeaning gl = new DFSGraphLeaning(leanerData,randomiseBindings,prune);
			
			// single threaded for now
			glr = gl.call();
			glr.depth++;
			if(glr.getCoreMap()==null){
				// no need to merge
				// (actually, this will probably never happen since higher
				// level call always returns a core map, even if an automorphism)
				glr.setCoreMap(homo);
			} else {
				// we (may) need to merge the two homomorphisms
				glr.setCoreMap(merge(homo,glr.getCoreMap()));
			}
			
		}

		glr.joins+=joins;
		glr.setSolutionCount(1);
		
		return glr;
	}

	private static HashMap<BNode, Node> merge(HashMap<BNode, Node> map1, Map<BNode, Node> map2) {
		// if a-> b in map1 and b->c in map2, output a->c, etc.
		HashMap<BNode,Node> merge = new HashMap<BNode,Node>();
		
		for(Map.Entry<BNode,Node> me:map1.entrySet()){
			Node f = map2.get(me.getValue());
			if(f==null){
				f = me.getValue();
			} 
			merge.put(me.getKey(),f);
		}
		return merge;
	}

	/**
	 * Tries to get the homomorphism with the fewest blank nodes 
	 * @param query
	 * @param initialMap 
	 * @return
	 * @throws InterruptedException 
	 */
	private HashMap<BNode,Node> getHomomorphism(ArrayList<Node[]> query, HashMap<BNode, Node> initialMap) throws InterruptedException{
		ArrayList<Node[]> queryCopy = new ArrayList<Node[]>(query);
		HashMap<BNode,Node> startingSolution = new HashMap<BNode,Node>();
		Count<Node> timesBound = new Count<Node>();
		
		if(initialMap!=null && !initialMap.isEmpty()){
			startingSolution.putAll(initialMap);
			for(Map.Entry<BNode, Node> e: initialMap.entrySet()){
				timesBound.add(e.getValue());
			}
		}
		HashMap<BNode,Node> hom = join(queryCopy, startingSolution, timesBound);
		return hom;
	}
	
	
	/**
	 * 
	 * @param current Pattern with blank node subject and object
	 * @param todo Patterns left to do
	 * @param partialSol A partial solution 
	 * @param timesBound Number of times each term bound in current partial solution
	 * @return null if no valid homomorphism found in depth first search, otherwise a single homomorphism
	 * @throws InterruptedException 
	 */
	private HashMap<BNode,Node> join(ArrayList<Node[]> todo, HashMap<BNode,Node> partialSol, Count<Node> timesBound) throws InterruptedException{
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		
		joins++;
		
		Node[] current = todo.remove(0);
		Bindings bindings = getBindings(current,partialSol,timesBound);
		if(bindings.getBindings()==null){
			return null;
		}
		
		// the following are all for pruning
		// by automorphism
		Orbits o = null;
		TreeSet<Node> visited = null;
		HashMap<BNode,Integer> indexes = null;
		HashMap<StaticArrayList<Integer>,ArrayList<Node>> partition = null;
		int checked = 0;
		
		// we only prune if one term is bound
		// the case for two terms is awkward :(
		if(prune && bindings.getOutput().size()==1){
			o = new Orbits();
			visited = new TreeSet<Node>();
			indexes = new HashMap<BNode,Integer>();
			int i=0;
			
			// this will be used to compute a signature
			// to determine which automorphisms are the identity
			// for nodes visited thus far
			for(BNode b:partialSol.keySet()){
				indexes.put(b, i++);
			}
			
//			// if two terms are bound, we'll arbitrarily 
//			// consider the second one fixed
//			if(bindings.getOutput().size()>1){
//				indexes.put(bindings.getOutput().get(1),i++);
//			}
			
			// only store one solution per rooted signature
			// ... will compose orbits from individual pairs
			partition = new HashMap<StaticArrayList<Integer>,ArrayList<Node>>();
		}
		
		for(Node[] bind : bindings.getBindings()){
			// first let's check if we can prune
			// this branch according to automorphisms
			
			// we should only check if
			// (1) precisely one term is bound
			// (2) binding constant is not in partialSol (i.e., this is an automorphism we're trying)
			// (3) we have something previous to map to
			if(prune && bindings.getOutput().size()==1 && !visited.isEmpty() && automorphisms!=null && automorphisms.size()>1 && !partialSol.containsKey(bind[0])){
//				System.err.println("Trying to prune");
//				System.err.println(automorphismMask);
//				for(ArrayList<Node> auto:automorphisms)
//					System.err.println(auto);
//				System.err.println("Root "+indexes.keySet());
//				System.err.println("Visited "+visited);
//				System.err.println("Next "+bind[0]);
				
				// let's check the orbits we know
				if(prune(bind[0],o,visited)){
					visited.add(bind[0]);
					continue;
				}
				
				// here we only check new automorphisms not used
				// to compute new orbits
				boolean skip = false;
				for(int i=checked; i<automorphisms.size(); i++){
					// we compute the indexes of the nodes we've
					// bound in partial solution: the automorphisms
					// we're looking for are the identity on these
					StaticArrayList<Integer> signature = new StaticArrayList<Integer>(indexes.size());
					for(int j=0; j<indexes.size(); j++){
						signature.add(-1);
					}
					for(int j=0; j<automorphisms.get(i).size(); j++){
						// value must be a bnode since it's an automorphism
						BNode b = (BNode) automorphisms.get(i).get(j);
						Integer bi = indexes.get(b);
						if(bi!=null){
							if(bi>=signature.size()){
								System.err.println(signature);
								System.err.println(indexes);
								System.err.println(Nodes.toN3(bind));
							}
							signature.set(bi,j);
						}
					}
					ArrayList<Node> rooted = partition.get(signature);
					if(rooted==null){
						partition.put(signature, automorphisms.get(i));
					} else{
						o.addAndCompose(getMapping(rooted,automorphisms.get(i)));
						
						if(prune(bind[0],o,visited)){
							skip = true;
//							System.err.println("... pruning");
							break;
						}
					}
				}
				
				checked = automorphisms.size()-1;
				visited.add(bind[0]);
				
				if(skip){
					continue;
				}
			} else if(prune && bindings.getOutput().size()==1 && !partialSol.containsKey(bind[0])){
				// even if conditions for pruning are met, we need
				// to keep track of which siblings we've visited
				visited.add(bind[0]);
			}
			
			
			// instead of copying, let's reuse the objects
			// update the inputs with new bindings
			for(int i=0; i<bindings.getOutput().size(); i++){
				partialSol.put(bindings.getOutput().get(i),bind[i]);
				timesBound.add(bind[i]);
			}

			if(!todo.isEmpty()){
				// recurse
				ArrayList<Node[]> nextTodo = new ArrayList<Node[]>(todo);
				HashMap<BNode,Node> sol = join(nextTodo,partialSol,timesBound);
				if(sol!=null) return sol;
			} else{
				// last pattern ... check to see if solution maps to
				// same term twice or maps to ground term
				HashSet<BNode> bnodes = getBNodeBindings(partialSol); 
				boolean homo = bnodes.size() < partialSol.size();
				
				// return first such homomorphism ... ordering should
				// pick a good one! (hopefully)
				if(homo) return partialSol;
				
				//otherwise we found an automorphism
				if(prune){
					if(automorphisms == null){
						automorphisms = new ArrayList<ArrayList<Node>>();
						automorphismMask = new ArrayList<BNode>();
						
						automorphismMask.addAll(partialSol.keySet());
						
						// adding the trivial automorphism makes life easier
						// later :)
						ArrayList<Node> automorphism = new ArrayList<Node>();
						automorphism.addAll(automorphismMask);
						automorphisms.add(automorphism);
					}
					
					boolean trivial = true;
					ArrayList<Node> automorphism = new ArrayList<Node>();
					for(BNode b: automorphismMask){
						Node val = partialSol.get(b);
						automorphism.add(val);
						if(!b.equals(val)){
							trivial = false;
						}
					}
					
					// don't want to add the trivial automorphism twice
					if(!trivial){
						automorphisms.add(automorphism);
					}
				}
			}
			
			// if solution is no good, we need to
			// return the object state to where it was
			for(int i=0; i<bindings.getOutput().size(); i++){
				partialSol.remove(bindings.getOutput().get(i));
				timesBound.replace(bind[i],timesBound.get(bind[i])-1);
			}
		}
			
		// no homomorphism found
		return null;
	}	
	
	private static boolean prune(Node next, Orbits o, TreeSet<Node> visited){
		// first check if we're in an orbit previously computed
		// it's sufficient to find an orbit for one of the bind terms
		TreeSet<Node> orbits = o.getNonTrivialOrbit(next);
		if(orbits!=null && orbits.size()>0){
			// if any visited node can be mapped to next
			// no need to visit this binding
			for(Node v:visited){
				if(!v.equals(next) && orbits.contains(v)){
					return true;
				}
			}
		}
		return false;
	}
	
	private static HashMap<Node, Node> getMapping(ArrayList<Node> a, ArrayList<Node> b) {
		HashMap<Node,Node> map = new HashMap<Node,Node>();
		// should be same size
		for(int i=0; i<Math.min(a.size(),b.size()); i++){
			map.put(a.get(i), b.get(i));
		}
		return map;
	}

	/**
	 * Just an arraylist that caches hashcodes.
	 * 
	 * Assumes of course that hashcode is first called only after data are loaded.
	 * 
	 * @author ahogan
	 *
	 * @param <E>
	 */
	public static class StaticArrayList<E> extends ArrayList<E>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 7336739836529651419L;
		public int hashCode = 0;
		
		public StaticArrayList(){
			super();
		}
		
		public StaticArrayList(int size){
			super(size);
		}
		
		public int hashCode(){
			if(hashCode==0){
				hashCode = super.hashCode();
				return hashCode;
			} else{
				return hashCode;
			}
		}
	}
		
	public static void main(String[] args) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("data/k-9.nt"));
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
		
		DFSGraphLeaning gl = new DFSGraphLeaning(triples,false,false);
		
		GraphLeaningResult glr = gl.call();
		
//		for(Node[] leanTriple: glr.leanData){
//			System.err.println(Nodes.toN3(leanTriple));
//		}
		
		System.err.println(glr.coreMap);
		System.err.println(glr.solCount);
		System.err.println(glr.depth);
		System.err.println(glr.joins);
		
//		System.err.println(toN3(glr.leanData));

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
		
		System.err.println(toN3(glr.leanData));
		
		System.err.println(triples.size()+" "+glr.leanData.size());
		
		
	}
	
}
