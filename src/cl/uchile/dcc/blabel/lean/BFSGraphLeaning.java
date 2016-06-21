package cl.uchile.dcc.blabel.lean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

import cl.uchile.dcc.blabel.lean.util.Bindings;

public class BFSGraphLeaning extends GraphLeaning{
	
	public static final Logger LOG = Logger.getLogger(BFSGraphLeaning.class.getName());
	
	public BFSGraphLeaning(Collection<Node[]> data){
		super(data);
	}
	
	/**
	 * Gets the homomorphism with the fewest blank nodes guaranteed
	 * @param query
	 * @return
	 * @throws InterruptedException 
	 * @throws Exception 
	 */
	protected GraphLeaningResult getCore(ArrayList<Node[]> query, HashMap<BNode,Node> initialMap) throws InterruptedException {
		ArrayList<HashMap<BNode,Node>> solutions = getSolutions(query,initialMap);
		
		int minCount = Integer.MAX_VALUE;
		HashMap<BNode,Node> coreSolution = null;
		// core given by a solution with fewest blank nodes
		for(HashMap<BNode,Node> solution:solutions){
			HashSet<BNode> bnodes = getBNodeBindings(solution);
			
			// fewest blank nodes and reduces at least one blank node
			if(bnodes.size()<minCount && bnodes.size()<solution.size()){
				coreSolution = solution;
				minCount = bnodes.size();
			}
		}
		
		Collection<Node[]> leanData = filteredData;
		
		if(coreSolution!=null){
			leanData = mapData(leanData,coreSolution);
		}
		
		GraphLeaningResult glr = new GraphLeaningResult(leanData);
		glr.setDepth(0);
		glr.setJoins(joins);
		glr.setSolutionCount(solutions.size());
		glr.setCoreMap(coreSolution);
		
		return glr;
	}
	
	private ArrayList<HashMap<BNode,Node>> getSolutions(ArrayList<Node[]> todo, HashMap<BNode, Node> initialMap) throws InterruptedException{
		ArrayList<HashMap<BNode,Node>> solutions = new ArrayList<HashMap<BNode,Node>>();
		if(initialMap!=null)
			solutions.add(initialMap);
		else solutions.add(new HashMap<BNode,Node>());
		ArrayList<Node[]> todoCopy = new ArrayList<Node[]>(todo);
		return join(todoCopy,solutions);
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
	private ArrayList<HashMap<BNode,Node>> join(ArrayList<Node[]> todo, ArrayList<HashMap<BNode,Node>> partialSols) throws InterruptedException{
		ArrayList<HashMap<BNode,Node>> nextPartialSols = new ArrayList<HashMap<BNode,Node>>();
		Node[] current = todo.remove(0);
		
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		
		for(HashMap<BNode,Node> partialSol:partialSols){
			joins++;
			Bindings bindings = getBindings(current,partialSol);
			
			if(bindings.getBindings()==null){
				continue;
			}
			
			if (Thread.interrupted()) {
				throw new InterruptedException();
			}
			
			for(Node[] bind : bindings.getBindings()){
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
				
				// copy all the inputs
				HashMap<BNode,Node> nextPartialSol = new HashMap<BNode,Node>();
				nextPartialSol.putAll(partialSol);
	
				// update the inputs with new bindings
				for(int i=0; i<bindings.getOutput().size(); i++){
					nextPartialSol.put(bindings.getOutput().get(i),bind[i]);
				}
				
				// add to table of partial solutions
				nextPartialSols.add(nextPartialSol);
			}
		}
		
		if(nextPartialSols.isEmpty()){
			// no solution found
			// should never happen?
			// should at least map to self
			LOG.warning("No solution found");
			return nextPartialSols;
		}
		
		if(!todo.isEmpty()){
			// recurse
			ArrayList<Node[]> nextTodo = new ArrayList<Node[]>(todo);
			ArrayList<HashMap<BNode,Node>> sol = join(nextTodo,nextPartialSols);
			return sol;
		} else{
			// we're done
			return nextPartialSols;
		}
	}
		
	public static void main(String[] args) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("data/grid.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/square.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/trivial-nonlean.nt"));
//		BufferedReader br = new BufferedReader(new FileReader("data/saramandai.nq"));
//		BufferedReader br = new BufferedReader(new FileReader("data/timeout.nt"));
		NxParser nxp = new NxParser(br);
		
		ArrayList<Node[]> triples = new ArrayList<Node[]>();
		
		while(nxp.hasNext()){
			triples.add(nxp.next());
		}
		
		br.close();
		
		BFSGraphLeaning gl = new BFSGraphLeaning(triples);
		
		GraphLeaningResult glr = gl.call();
		
		for(Node[] leanTriple: glr.leanData){
			System.err.println(Nodes.toN3(leanTriple));
		}
		
		System.err.println(glr.coreMap);
		System.err.println(glr.solCount);
		System.err.println(glr.depth);
		System.err.println(glr.joins);
		
		System.err.println(triples.size()+" "+glr.leanData.size());
	}
	
}
