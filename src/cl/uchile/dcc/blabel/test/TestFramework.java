package cl.uchile.dcc.blabel.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.parser.NxParser;

import com.sun.istack.internal.logging.Logger;

import cl.uchile.dcc.blabel.label.GraphColouring;
import cl.uchile.dcc.blabel.label.GraphLabelling;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingArgs;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingResult;
import cl.uchile.dcc.blabel.lean.BFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.DFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning.GraphLeaningResult;
import cl.uchile.dcc.blabel.test.TestFramework.TestFrameworkResult;

public class TestFramework implements Callable<TestFrameworkResult> {
	static final Logger LOG = Logger.getLogger(TestFramework.class);
	
	private final TestFrameworkArgs tfa; 
	
	private final Collection<Node[]> data;
	
	private static final String BNODE_PREFIX = "b";
	
	private static final Random RAND = new Random();
	
	public static String LABEL_TEST = "lab";
	public static String LABEL_NO_PRUNING_TEST = "lab_np";
	
	public static String DFS_TEST = "dfs_ord";
	public static String DFS_RANDOM_TEST = "dfs_rand";
	public static String BFS_TEST = "bfs";
	
	
	public TestFramework(Collection<Node[]> data){
		this(data, new TestFrameworkArgs());
	}
	
	public TestFramework(Collection<Node[]> data, TestFrameworkArgs tfa){
		this.data = data;
		this.tfa = tfa;
	}
	
	@Override
	public TestFrameworkResult call() throws Exception {
		TestFrameworkResult tfr = new TestFrameworkResult();
		
		// TEST 1: compare labelling results
		// shuffling every time
		TreeMap<TreeSet<Node[]>,TreeSet<String>> labellingComparisons = new TreeMap<TreeSet<Node[]>,TreeSet<String>>(GraphColouring.GRAPH_COMP);
		TreeSet<String> labellingExceptions = new TreeSet<String>();
		for(int i=0; i<tfa.shuffles; i++){
			ArrayList<Node[]> data = new ArrayList<Node[]>();
			if(i==0){
				data.addAll(this.data);
			} else{
				data = renameBnodesAndShuffle(this.data);
			}
			
			// compare with pruning and without
			tfa.getLabellingArgs().setPrune(true);
			testLabel(data,tfa.getLabellingArgs(),tfa,LABEL_TEST+"i:"+i,labellingComparisons,labellingExceptions);
			
			tfa.getLabellingArgs().setPrune(false);
			testLabel(data,tfa.getLabellingArgs(),tfa,LABEL_NO_PRUNING_TEST+"i:"+i,labellingComparisons,labellingExceptions);
		}
		tfr.setLabellingComparisons(labellingComparisons);
		tfr.setLabellingExceptions(labellingExceptions);
		
		
		// TEST 2: compare leaning results
		// shuffling every time and recursing once
		// to see if answer can be leaned further
		// ... also tests the the coreMap is complete and correct
		TreeMap<TreeSet<Node[]>,TreeSet<String>> leaningComparisons = new TreeMap<TreeSet<Node[]>,TreeSet<String>>(GraphColouring.GRAPH_COMP);
		// for timeouts and OOM exceptions, etc.
		TreeSet<String> leaningExceptions = new TreeSet<String>();
		// for errors where map does not give lean output
		// or is not complete
		TreeSet<String> mappingsFailures = new TreeSet<String>();
		for(int i=0; i<tfa.shuffles; i++){
			ArrayList<Node[]> data = new ArrayList<Node[]>();
			if(i==0){
				data.addAll(this.data);
			} else{
				data = renameBnodesAndShuffle(this.data);
			}
			
			testLeanAndLabel(data,DFS_TEST+"i:"+i,tfa,leaningComparisons,leaningExceptions,mappingsFailures,true);
			testLeanAndLabel(data,DFS_RANDOM_TEST+"i:"+i,tfa,leaningComparisons,leaningExceptions,mappingsFailures,true);
			testLeanAndLabel(data,BFS_TEST+"i:"+i,tfa,leaningComparisons,leaningExceptions,mappingsFailures,true);
		}
		tfr.setLeaningComparisons(leaningComparisons);
		tfr.setLeaningExceptions(leaningExceptions);
		tfr.setMappingsFailures(mappingsFailures);
		return tfr;
	}
	
	public static boolean testLabel(Collection<Node[]> data, GraphLabellingArgs la, TestFrameworkArgs tfa, String testname,
			TreeMap<TreeSet<Node[]>, TreeSet<String>> comparisons, TreeSet<String> exceptions) throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		
		
		GraphLabelling l = new GraphLabelling(data, la);
		GraphLabellingResult lr = null;
		try{
			lr = run(l,tfa.timeout);
		} catch(Exception e){
			exceptions.add(testname);
			return false;
		}
		
//		for(Node[] na:lr.getGraph()){
//			System.err.println("\t"+testname+"\t"+Nodes.toN3(na));
//		}
		
		TreeSet<String> tests = comparisons.get(lr.getGraph());
		if(tests==null){
			tests = new TreeSet<String>();
			comparisons.put(lr.getGraph(), tests);
		}
		tests.add(testname);
		return true;
	}
	
	public static boolean testLeanAndLabel(Collection<Node[]> data, String testname, TestFrameworkArgs tfa, TreeMap<TreeSet<Node[]>, TreeSet<String>> comparisons, TreeSet<String> exceptions, TreeSet<String> mappingsFailures, boolean recurse) throws InterruptedException, ExecutionException, TimeoutException{
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}
		
		GraphLeaning gl = null;
		if(testname.startsWith(DFS_TEST)){
			gl = new DFSGraphLeaning(data);
		} else if(testname.startsWith(DFS_RANDOM_TEST)){
			gl = new DFSGraphLeaning(data,false);
		} else{
			gl = new BFSGraphLeaning(data);
		}
		
		// lean data per DFS 
		GraphLeaningResult glr = null;
		try{
			glr = run(gl,tfa.timeout);
		} catch(Exception e){
			exceptions.add(testname+"_lean");
			return false;
		}
		
		testMapping(data,glr.getLeanData(),glr.getCoreMap(),testname,mappingsFailures);

		// compute canonical labelling of result
		testLabel(glr.getLeanData(),tfa.getLabellingArgs(),tfa,testname+"_label",comparisons,exceptions);
		
		// tests to see if result of leaning again
		// is the same
		if(recurse){
			return testLeanAndLabel(glr.getLeanData(), testname+"+depth2", tfa, comparisons, exceptions, mappingsFailures, false);
		}
		
		return true;
	}
	
	private static void testMapping(Collection<Node[]> data, Collection<Node[]> leanData, Map<BNode, Node> coreMap, String testname, TreeSet<String> mappingsFailures) {
		if(getBnodes(data).size()!=coreMap.size()){
			mappingsFailures.add(testname);
		} else{
			TreeSet<Node[]> mapData = GraphLeaning.mapData(data, coreMap);
			TreeSet<Node[]> leanDataSet = new TreeSet<Node[]>(NodeComparator.NC);
			leanDataSet.addAll(leanData);
			if(GraphColouring.GRAPH_COMP.compare(mapData,leanDataSet)!=0){
				mappingsFailures.add(testname);
			}
		}
		
	}

	private static HashSet<BNode> getBnodes(Collection<Node[]> data) {
		HashSet<BNode> bnodes = new HashSet<BNode>();
		for(Node[] d: data){
			for(int i: new int[]{0,2}){
				if(d[i] instanceof BNode){
					bnodes.add((BNode)d[i]);
				}
			}
		}
		return bnodes;
	}

	public static <E> E run(Callable<E> gl, long timeout) throws InterruptedException, ExecutionException, TimeoutException{
		ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<E> future = executor.submit(gl);
        
        E glr = future.get(timeout, TimeUnit.SECONDS);
        executor.shutdownNow();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        return glr;
	}
	
	public static ArrayList<Node[]> renameBnodesAndShuffle(Collection<Node[]> data){
		 ArrayList<Node[]> shuffled = new ArrayList<Node[]>();
		 HashMap<BNode,BNode> renaming = new HashMap<BNode,BNode>();
		 HashSet<BNode> used = new HashSet<BNode>();
		 
		 
		 for(Node[] d:data){
			 Node[] copy = new Node[d.length];
			 System.arraycopy(d, 0, copy, 0, d.length);
			 
			 for(int i : new int[]{0,2}){
				 if(d[i] instanceof BNode){
					 BNode r = renaming.get(d[i]);
					 if(r==null){
						 do{
							 r = new BNode(BNODE_PREFIX+RAND.nextInt());
						 } while(used.add(r));
						 renaming.put((BNode)d[i], r);
					 }
					copy[i] = r;
				 }
			 }
			 
			 shuffled.add(copy);
		 }
		 
		 Collections.shuffle(shuffled);
		 
		 return shuffled;
	}

	
	public static class TestFrameworkResult{
		TreeMap<TreeSet<Node[]>,TreeSet<String>> labellingComparisons;
		TreeSet<String> labellingExceptions;
		TreeMap<TreeSet<Node[]>,TreeSet<String>> leaningComparisons;
		TreeSet<String> leaningExceptions;
		TreeSet<String> mappingsFailures;
		
		
		public TestFrameworkResult(){
			;
		}


		public TreeMap<TreeSet<Node[]>, TreeSet<String>> getLabellingComparisons() {
			return labellingComparisons;
		}


		public void setLabellingComparisons(TreeMap<TreeSet<Node[]>, TreeSet<String>> labellingComparisons) {
			this.labellingComparisons = labellingComparisons;
		}


		public TreeSet<String> getLabellingExceptions() {
			return labellingExceptions;
		}


		public void setLabellingExceptions(TreeSet<String> labellingExceptions) {
			this.labellingExceptions = labellingExceptions;
		}


		public TreeMap<TreeSet<Node[]>, TreeSet<String>> getLeaningComparisons() {
			return leaningComparisons;
		}


		public void setLeaningComparisons(TreeMap<TreeSet<Node[]>, TreeSet<String>> leaningComparisons) {
			this.leaningComparisons = leaningComparisons;
		}


		public TreeSet<String> getLeaningExceptions() {
			return leaningExceptions;
		}


		public void setLeaningExceptions(TreeSet<String> leaningExceptions) {
			this.leaningExceptions = leaningExceptions;
		}


		public TreeSet<String> getMappingsFailures() {
			return mappingsFailures;
		}


		public void setMappingsFailures(TreeSet<String> mappingsFailures) {
			this.mappingsFailures = mappingsFailures;
		}
	}
	
	public static class TestFrameworkArgs{
		public static final long DEFAULT_TIMEOUT = 60;
		public static final int DEFAULT_SHUFFLES = 3;
		public static final GraphLabellingArgs DEFAULT_LABELLING_ARGS = new GraphLabellingArgs();
		
		long timeout = DEFAULT_TIMEOUT;
		int shuffles = DEFAULT_SHUFFLES;
		GraphLabellingArgs labellingArgs = DEFAULT_LABELLING_ARGS;
		
		public TestFrameworkArgs(){
			;
		}

		public long getTimeout() {
			return timeout;
		}

		public void setTimeout(long timeout) {
			this.timeout = timeout;
		}

		public int getShuffles() {
			return shuffles;
		}

		public void setShuffles(int shuffles) {
			this.shuffles = shuffles;
		}

		public GraphLabellingArgs getLabellingArgs() {
			return labellingArgs;
		}

		public void setLabellingArgs(GraphLabellingArgs labellingArgs) {
			this.labellingArgs = labellingArgs;
		}
	}
	
	public static void main(String[] args) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader("data/grid.nt"));
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
		
//		TestFrameworkArgs tfa = new TestFrameworkArgs();
		TestFramework tf = new TestFramework(triples);
		
		TestFrameworkResult tfr = tf.call();
		
		System.err.println("Labelling exceptions "+tfr.labellingExceptions);
		System.err.println("Labelling partition sizes "+tfr.labellingComparisons.size());
		if(tfr.labellingComparisons.size()>1){
			System.err.println("Labelling partitions: "+tfr.labellingComparisons.values());
		}
		
		System.err.println("Leaning exceptions "+tfr.leaningExceptions);
		System.err.println("Leaning partition sizes "+tfr.leaningComparisons.size());
		if(tfr.leaningComparisons.size()>1){
			System.err.println("Leaning partitions: "+tfr.leaningComparisons.values());
		}
		System.err.println("Mapping exceptions "+tfr.mappingsFailures);
		
	}
}
