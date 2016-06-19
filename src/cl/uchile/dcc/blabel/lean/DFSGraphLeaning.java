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
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.stats.Count;

import cl.uchile.dcc.blabel.lean.util.Bindings;

public class DFSGraphLeaning extends GraphLeaning{
	
	public static final Logger LOG = Logger.getLogger(DFSGraphLeaning.class.getName());
	
	public DFSGraphLeaning(Collection<Node[]> data){
		super(data);
	}
	
	public DFSGraphLeaning(Collection<Node[]> data, boolean randomiseBindings){
		super(data,randomiseBindings);
	}
	
	/**
	 * Gets the homomorphism with the fewest blank nodes guaranteed
	 * @param query
	 * @return
	 * @throws InterruptedException 
	 * @throws Exception 
	 */
	protected GraphLeaningResult getCore(ArrayList<Node[]> query) throws InterruptedException {
		HashMap<BNode,Node> homo = getHomomorphism(query);
		GraphLeaningResult glr = null;
		if(homo==null){
			// graph is lean, we only have automorphisms
			glr = new GraphLeaningResult(data);
			glr.depth = 1;
		} else{
			//TODO we could probably be more efficient here
			// reusing some work and doing some better checks
			
			// map the data per the homomorphism we found
			// and recurse on everything
			TreeSet<Node[]> leanerData = mapData(data,homo);
			DFSGraphLeaning gl = new DFSGraphLeaning(leanerData);
			
			// single threaded for now
			glr = gl.call();
			glr.depth++;
			if(glr.getCoreMap()==null){
				// no better mapping
				glr.setCoreMap(homo);
			} else {
				// we need to merge the two homomorphisms
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
		return null;
	}

	/**
	 * Tries to get the homomorphism with the fewest blank nodes 
	 * @param query
	 * @return
	 * @throws InterruptedException 
	 */
	private HashMap<BNode,Node> getHomomorphism(ArrayList<Node[]> query) throws InterruptedException{
		ArrayList<Node[]> queryCopy = new ArrayList<Node[]>(query);
		HashMap<BNode,Node> hom = join(queryCopy, new HashMap<BNode,Node>(), new Count<Node>());
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
		
		for(Node[] bind : bindings.getBindings()){
			// copy all the inputs
			HashMap<BNode,Node> nextPartialSol = new HashMap<BNode,Node>();
			nextPartialSol.putAll(partialSol);

			Count<Node> nextTimesBound = new Count<Node>();
			nextTimesBound.putAll(timesBound);

			// update the inputs with new bindings
			for(int i=0; i<bindings.getOutput().size(); i++){
				nextPartialSol.put(bindings.getOutput().get(i),bind[i]);
				nextTimesBound.add(bind[i]);
			}

			if(!todo.isEmpty()){
				// recurse
				ArrayList<Node[]> nextTodo = new ArrayList<Node[]>(todo);
				HashMap<BNode,Node> sol = join(nextTodo,nextPartialSol,nextTimesBound);
				if(sol!=null) return sol;
			} else{
				// last pattern ... check to see if solution maps to
				// same term twice or maps to ground term
				HashSet<BNode> bnodes = getBNodeBindings(nextPartialSol); 
				boolean homo = bnodes.size() < nextPartialSol.size();
				
				// return first such homomorphism ... ordering should
				// pick a good one! (hopefully)
				if(homo) return nextPartialSol;
			}
		}
			
		// no homomorphism found
		return null;
	}	
		
	public static void main(String[] args) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("data/saramandai.nq"));
//		BufferedReader br = new BufferedReader(new FileReader("data/null-test2.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/timeout.nt"));
		NxParser nxp = new NxParser(br);
		

		TreeSet<Node[]> triples = new TreeSet<Node[]>(NodeComparator.NC);
		
		while(nxp.hasNext()){
			Node[] triple = nxp.next();
			triples.add(new Node[]{triple[0],triple[1],triple[2]});
		}
		
		br.close();
		
		DFSGraphLeaning gl = new DFSGraphLeaning(triples);
		
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
