package cl.uchile.dcc.blabel.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.FlyweightNodeIterator;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import cl.uchile.dcc.blabel.cli.RunSyntheticEvaluation.Benchmark;
import cl.uchile.dcc.blabel.label.GraphLabelling;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingArgs;
import cl.uchile.dcc.blabel.label.GraphLabelling.GraphLabellingResult;
import cl.uchile.dcc.blabel.lean.BFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.DFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning.GraphLeaningResult;

public class RunNQuadsTest {
	static Logger LOG = Logger.getLogger(RunNQuadsTest.class.getSimpleName());
	public static final Level LOG_LEVEL = Level.WARNING;
	static{
		for(Handler h : LOG.getParent().getHandlers()){
		    if(h instanceof ConsoleHandler){
		        h.setLevel(LOG_LEVEL);
		    }
		} 
		LOG.setLevel(LOG_LEVEL);
	}
	
	public static int TICKS = 10000000;
	public static int FW = 100000;
	
	public static final int DEFAULT_TIMEOUT = 600; //in seconds
	
	public static void main(String[] args) throws IOException, InterruptedException{
		long b4 = System.currentTimeMillis();
		
		Option inO = new Option("i", "input file");
		inO.setArgs(1);
		inO.setRequired(true);
		
		Option ingzO = new Option("igz", "input file is GZipped");
		ingzO.setArgs(0);
		
		Option helpO = new Option("h", "print help");
		
		Option tO = new Option("t", "timeout for each test in seconds (default "+DEFAULT_TIMEOUT+")");
		tO.setArgs(1);
		
		Option sO = new Option("s", "hashing scheme: 0:md5 1:murmur3_128 2:sha1 3:sha256 4:sha512");
		sO.setArgs(1);
		
		Option lO = new Option("l", "leaning algorithm: 0:dfs 1:bfs");
		lO.setArgs(1);
		
		Option rO = new Option("r", "randomise dfs search (don't guess best, select random)");
		
		Option dO = new Option("d", "if running isomorphism labelling, count duplicate graphs");
		
		Option bO = new Option("b", "select the process to run: "+RunSyntheticEvaluation.BENCHMARK_OPTIONS);
		bO.setArgs(1);
		bO.setRequired(true);
				
		Options options = new Options();
		options.addOption(inO);
		options.addOption(ingzO);
		options.addOption(sO);
		options.addOption(lO);
		options.addOption(rO);
		options.addOption(dO);
		options.addOption(bO);
		options.addOption(tO);
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		// print help options and return
		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}
		
		int timeout = DEFAULT_TIMEOUT;
		if(cmd.hasOption("t")){
			timeout = Integer.parseInt(cmd.getOptionValue("t"));
		}
		
		int benchId = Integer.parseInt(cmd.getOptionValue("b"));
		Benchmark bench = Benchmark.values()[benchId]; 
		
		boolean randomiseDfs = cmd.hasOption("r");
		
		boolean countDupes = cmd.hasOption("d");
		
		HashFunction hf = null;
		int s = -1;
		if(bench.equals(Benchmark.LABEL) || bench.equals(Benchmark.BOTH)){
			s = Integer.parseInt(cmd.getOptionValue("s"));
			switch(s){
				case 0: hf = Hashing.md5(); break;
				case 1: hf = Hashing.murmur3_128(); break;
				case 2: hf = Hashing.sha1(); break;
				case 3: hf = Hashing.sha256(); break;
				case 4: hf = Hashing.sha512(); break;
			}
		}
		
		int l = -1;
		if(bench.equals(Benchmark.LEAN) || bench.equals(Benchmark.BOTH)){
			l = Integer.parseInt(cmd.getOptionValue("l"));
		}
		
		InputStream is = new FileInputStream(cmd.getOptionValue("i"));
		if(cmd.hasOption("igz"))
			is = new GZIPInputStream(is);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		NxParser nxp = new NxParser(br);
		
		if(!nxp.hasNext()){
			LOG.info("Empty input");
			return;
		}
		
		Node old = null;
		Node[] stmt = null;
		boolean done = false; 
		
		Map<HashCode,TreeSet<Node>> dupeGraphs = new HashMap<HashCode,TreeSet<Node>>();
		
		long slowestTime = 0;
		Node slowestGraph = null;
		
		int failed = 0;
		int doc = 0;
		int docwb = 0;
		long read = 0;
		
		LOG.info("Starting process ...");
		
		System.out.println("===============================================");
		System.out.println("Input file:\t"+cmd.getOptionValue("i"));
		System.out.println("===============================================");
		System.out.println("Benchmark:\t"+bench.toString());
		System.out.println("===============================================");
		System.out.println("Timeout:\t"+timeout);
		System.out.println("===============================================");
		if(l!=-1){
			if(l==0){
				System.out.println("Running DFS leaning algorithm, random: "+randomiseDfs);
				System.out.println("===============================================");
			} else if(l==1) {
				System.out.println("Running BFS leaning algorithm");
				System.out.println("===============================================");
			} else {
				LOG.info("Illegal value for parameter l:"+l);
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("parameters:", options );
				br.close();
				return;
			}
		}
		if(hf!=null){
			System.out.println("Hashing:\t"+hf.getClass().getSimpleName());
			System.out.println("===============================================");
			System.out.println("Counting duplicate isomorphic graphs:\t"+countDupes);
			System.out.println("===============================================");
		}
		System.out.println("Timestamp started:\t"+b4);
		System.out.println("===============================================");
		
		
		Iterator<Node[]> iter = nxp;
		
		// re-use node references: pushes down mem
		// at cost of map lookups
		iter = new FlyweightNodeIterator(FW,iter);
		
		Collection<Node[]> data = new ArrayList<Node[]>();
		
		HashSet<BNode> bnodes = new HashSet<BNode>();
		
		while(!done){
			done = !iter.hasNext();
			if(!done){
				// read next line
				stmt = iter.next();
				read++;
				
				if(read%TICKS==0){
					LOG.info("Read "+read+" input statements and "+doc+" documents");
				}
			}
			
			
			if(done || (old!=null && !stmt[3].equals(old))){
				// end of document ... time to canonicalise
				doc++;
				
				
				boolean fail = false;
				if(bnodes.isEmpty()){
					// no blank nodes ... nothing to do
					System.out.println("NOBNODES\t"+old+"\t"+data.size()+"0\t");
				} else{
					docwb++;
					
					int bnodeCount = bnodes.size();
					
					long duration = 0;
					
					if(bench.equals(Benchmark.LEAN) || bench.equals(Benchmark.BOTH)){
						GraphLeaning gl = null;
						if(l==0){
							LOG.info("Running DFS leaning algorithm, random: "+randomiseDfs);
							gl = new DFSGraphLeaning(data,randomiseDfs);
						} else if(l==1) {
							LOG.info("Running BFS leaning algorithm");
							gl = new BFSGraphLeaning(data);
						}
						
						ExecutorService executor = Executors.newSingleThreadExecutor();
				        Future<GraphLeaningResult> future = executor.submit(gl);
				        
				        try {
				        	long b4l = System.currentTimeMillis();
				            LOG.info("Running leaning ...");
				            GraphLeaningResult glr = future.get(timeout, TimeUnit.SECONDS);
				            LOG.info("... finished!");
				            
				            int leanBnodeCount = RunSyntheticEvaluation.countBnodes(glr.getLeanData());
				            long runtime = System.currentTimeMillis()-b4l;
				            duration += runtime;
				            System.out.println("LEAN\t"+old+"\t"+"\t"+data.size()+"\t"+bnodeCount+"\t"+runtime+"\t"+glr.getLeanData().size()+"\t"+leanBnodeCount+"\t"+glr.getJoins()+"\t"+glr.getDepth()+"\t"+glr.getSolutionCount()+"\t"+(data.size()-glr.getLeanData().size())+"\t"+(bnodeCount-leanBnodeCount));
				            
				            if(bench.equals(Benchmark.BOTH)){
				            	data = glr.getLeanData();
					        	bnodeCount = leanBnodeCount;
					        }
				        } catch (Exception e) {
				        	System.out.println("LEAN\t"+old+"\t"+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());//+"\t"+gc.getTotalColourIterations()+"\t"+gc.getLeaves().countLeaves()+"\t"+gc.getLeaves().getAutomorphismGroup().countOrbits()+"\t"+gc.getLeaves().getAutomorphismGroup().maxOrbit());
				        	LOG.warning(e.getClass().getName()+": "+e.getMessage());
				        	
				        	fail = true;
				        }
				        executor.shutdownNow();
				        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
					}
					
					if(!fail && (bench.equals(Benchmark.LABEL) || bench.equals(Benchmark.BOTH))){
						GraphLabellingArgs cla = new GraphLabellingArgs();
						cla.setHashFunction(hf);
						
						GraphLabelling cl = new GraphLabelling(data,cla);
						
						ExecutorService executor = Executors.newSingleThreadExecutor();
				        Future<GraphLabellingResult> future = executor.submit(cl);

				        try {
				        	long b4l = System.currentTimeMillis();
				            LOG.info("Running labelling ...");
				            GraphLabellingResult clr = future.get(timeout, TimeUnit.SECONDS);
				            LOG.info("... finished!");
				            
				            long runtime = System.currentTimeMillis()-b4l;
				            duration += runtime;
				            System.out.println("LABEL\t"+old+"\t"+"\t"+data.size()+"\t"+clr.getBnodeCount()+"\t"+runtime+"\t"+clr.getColourIterationCount()+"\t"+clr.getLeafCount());
				            
				            if(countDupes){
				            	HashCode hc = clr.getUniqueGraphHash();
				            	TreeSet<Node> dupes = dupeGraphs.get(hc);
					        	if(dupes==null){
					        		dupes = new TreeSet<Node>();
					        		dupeGraphs.put(hc, dupes);
					        	}
					        	dupes.add(old);
					        }
				        } catch (Exception e) {
				        	System.out.println("LABEL\t"+old+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());//+"\t"+gc.getTotalColourIterations()+"\t"+gc.getLeaves().countLeaves()+"\t"+gc.getLeaves().getAutomorphismGroup().countOrbits()+"\t"+gc.getLeaves().getAutomorphismGroup().maxOrbit());
				        	LOG.warning(e.getClass().getName()+": "+e.getMessage());
				        	
				        	fail = true; // skip to next class
				        } 
				        executor.shutdownNow();
				        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
					}
					
					if(duration>slowestTime){
						slowestTime = duration;
						slowestGraph = old;
					}
					if(fail){
						failed++;
					}
				}
				data.clear();
				bnodes.clear();
			} 

			if(!done){
				data.add(stmt);
				old = stmt[3];
				if(stmt[0] instanceof BNode){
					bnodes.add((BNode)stmt[0]);
				}
				
				if(stmt[2] instanceof BNode){
					bnodes.add((BNode)stmt[2]);
				}
			}
			
		}
		
		LOG.info("Finished! Read "+read+" input statements and "+doc+" documents.");
		
		if(countDupes && (bench.equals(Benchmark.LABEL) || bench.equals(Benchmark.BOTH))){
			LOG.info("Sorting duplicate graphs by size of class ...");
			TreeSet<TreeSet<Node>> sortedDupes = new TreeSet<TreeSet<Node>>(new BiggestTreeSetComparator());
			sortedDupes.addAll(dupeGraphs.values());
			LOG.info("... done.");
			
			System.out.println("===============================================");
			System.out.println("Duplicate graphs");
			for(TreeSet<Node> dupe:sortedDupes){
				if(dupe.size()>1){
					System.out.println(dupe);
				}
			}
			
			System.out.println("===============================================");
			System.out.println("Number of unique documents read with blank nodes:\t"+sortedDupes.size());
			System.out.println("===============================================");
			System.out.println("Largest collection of isomorphic documents with blank nodes:\t"+sortedDupes.first().size());
		}
		
		long end = System.currentTimeMillis();
		System.out.println("===============================================");
		System.out.println("Number of statements read:\t"+read);
		System.out.println("===============================================");
		System.out.println("Number of documents read:\t"+doc);
		System.out.println("===============================================");
		System.out.println("Number of documents read with blank nodes:\t"+docwb);
		System.out.println("===============================================");
		System.out.println("Number of failures:\t"+failed);
		System.out.println("===============================================");
		System.out.println("Slowest time:\t"+slowestTime);
		System.out.println("===============================================");
		System.out.println("Slowest graph:\t"+slowestGraph);
		System.out.println("===============================================");
		System.out.println("Total duration (ms):\t"+(end-b4));
		System.out.println("===============================================");
		System.out.println("Timestamp finished (ms):\t"+end);
		System.out.println("===============================================");
		
		br.close();
		
		LOG.info("Finished in "+(System.currentTimeMillis()-b4));
	}
	
	public static class BiggestTreeSetComparator implements Comparator<TreeSet<Node>>{

		@Override
		public int compare(TreeSet<Node> o1, TreeSet<Node> o2) {
			int comp = o2.size() - o1.size();
			if(comp!=0) return comp;
			
			if(o1.isEmpty())
				return 0;
			
			return o1.first().compareTo(o2.first());
		}
		
	}
}
