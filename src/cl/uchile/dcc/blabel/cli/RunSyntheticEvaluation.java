package cl.uchile.dcc.blabel.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import cl.uchile.dcc.blabel.label.GraphLabelling;
import cl.uchile.dcc.blabel.label.GraphLabelling.CanonicalLabellingArgs;
import cl.uchile.dcc.blabel.label.GraphLabelling.CanonicalLabellingResult;
import cl.uchile.dcc.blabel.lean.BFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.DFSGraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning;
import cl.uchile.dcc.blabel.lean.GraphLeaning.GraphLeaningResult;

public class RunSyntheticEvaluation {
	public static final Level LOG_LEVEL = Level.INFO;
	public static final Logger LOG = Logger.getLogger(RunSyntheticEvaluation.class.getName());
	static{
		for(Handler h : LOG.getParent().getHandlers()){
		    if(h instanceof ConsoleHandler){
		        h.setLevel(LOG_LEVEL);
		    }
		} 
		LOG.setLevel(LOG_LEVEL);
	}	
//	public static final String ZIP_LOC = "http://pallini.di.uniroma1.it/library/undirected_dim.zip";
//	public static final String ZIP_FN = "undirected_dim.zip";
	
	public static final int BUFFER = 8*1024;
	
	public static final int DEFAULT_TIMEOUT = 600; //in seconds
	
	public static final Resource PRED = new Resource("p");
	
	public static enum Benchmark { LEAN, LABEL, BOTH, CONTROL };
	
	public static String BENCHMARK_OPTIONS;
	static{
		StringBuilder sb = new StringBuilder();
		for(Benchmark b: Benchmark.values()){
			sb.append(b.toString()+":"+b.ordinal()+" ");
		}
		BENCHMARK_OPTIONS = sb.toString().trim();
	}

	public static void main(String[] args) throws IOException, InterruptedException{
//		Option zO = new Option("z", "fetch zip (doesn't seem to work :/)");
//		zO.setArgs(0);

		Option dO = new Option("d", "benchmark dir");
		dO.setArgs(1);
		dO.setRequired(true);

		Option sO = new Option("s", "hashing scheme: 0:md5 1:murmur3_128 2:sha1 3:sha256 4:sha512");
		sO.setArgs(1);
		
		Option lO = new Option("l", "leaning algorithm: 0:dfs 1:bfs");
		lO.setArgs(1);
		
		Option rO = new Option("r", "randomise dfs search (don't guess best, select random)");
		
		Option tO = new Option("t", "timeout for each test in seconds (default "+DEFAULT_TIMEOUT+")");
		tO.setArgs(1);
		
		Option bO = new Option("b", "benchmark to run: "+BENCHMARK_OPTIONS);
		bO.setArgs(1);
		bO.setRequired(true);

		Option helpO = new Option("h", "print help");

		Options options = new Options();
//		options.addOption(zO);
		options.addOption(dO);
		options.addOption(sO);
		options.addOption(lO);
		options.addOption(rO);
		options.addOption(tO);
		options.addOption(bO);
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
		
		int benchId = Integer.parseInt(cmd.getOptionValue("b"));
		Benchmark bench = Benchmark.values()[benchId]; 

		String dir = cmd.getOptionValue("d");

//		if(cmd.hasOption("z")){
//			String fn = downloadZip(ZIP_LOC,dir);
//			expandZip(fn);
//		}
		
		int timeout = DEFAULT_TIMEOUT;
		if(cmd.hasOption("t")){
			timeout = Integer.parseInt(cmd.getOptionValue("t"));
		}
		
		boolean randomiseDfs = cmd.hasOption("r");
		
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
		
		
		LOG.info("Building test-cases");
		File dirF = new File(dir);
		TreeMap<String,TreeMap<Integer,File>> testCases = buildTestcases(dirF);
		
		for(Map.Entry<String,TreeMap<Integer,File>> testClass : testCases.entrySet()){
			boolean fail = false;
			for(Map.Entry<Integer,File> classInstance : testClass.getValue().entrySet()){
				File fn = classInstance.getValue();
				
				LOG.info("Running class "+testClass.getKey()+" for k="+classInstance.getKey());
				LOG.info("Loading "+fn+" ...");
				Collection<Node[]> data = loadAndConvert(fn);
				LOG.info("... loaded "+data.size()+" undirected triples.");
				
		        // in case test fails, we still want to
				// have a bnode count
				int bnodeCount = countBnodes(data);
				
				LOG.info("... with "+bnodeCount+" blank nodes.");
				
				LOG.info("... running benchmark "+bench);
				
				
				if(bench.equals(Benchmark.LEAN) || bench.equals(Benchmark.BOTH)){
					GraphLeaning gl = null;
					if(l==0){
						LOG.info("Running DFS leaning algorithm, random: "+randomiseDfs);
						gl = new DFSGraphLeaning(data,randomiseDfs);
					} else if(l==1) {
						LOG.info("Running BFS leaning algorithm");
						gl = new BFSGraphLeaning(data);
					} else {
						LOG.info("Illegal value for parameter l:"+l);
						HelpFormatter formatter = new HelpFormatter();
						formatter.printHelp("parameters:", options );
						return;
					}
					
					ExecutorService executor = Executors.newSingleThreadExecutor();
			        Future<GraphLeaningResult> future = executor.submit(gl);
			        
			        try {
			        	long b4 = System.currentTimeMillis();
			            LOG.info("Running leaning ...");
			            GraphLeaningResult glr = future.get(timeout, TimeUnit.SECONDS);
			            LOG.info("... finished!");
			            
			            int leanBnodeCount = countBnodes(glr.getLeanData());
			            System.out.println("LEAN\t"+fn.getName()+"\t"+testClass.getKey()+"\t"+classInstance.getKey()+"\t"+data.size()+"\t"+bnodeCount+"\t"+(System.currentTimeMillis()-b4)+"\t"+glr.getLeanData().size()+"\t"+leanBnodeCount+"\t"+glr.getJoins()+"\t"+glr.getDepth()+"\t"+glr.getSolutionCount()+"\t"+(data.size()-glr.getLeanData().size())+"\t"+(bnodeCount-leanBnodeCount));
			            
			            if(bench.equals(Benchmark.BOTH)){
			            	data = glr.getLeanData();
				        	bnodeCount = leanBnodeCount;
				        }
			        } catch (Exception e) {
			        	LOG.info(e.getClass().getName()+" "+e.getMessage());
			        	System.out.println("LEAN\t"+fn.getName()+"\t"+testClass.getKey()+"\t"+classInstance.getKey()+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());//+"\t"+gc.getTotalColourIterations()+"\t"+gc.getLeaves().countLeaves()+"\t"+gc.getLeaves().getAutomorphismGroup().countOrbits()+"\t"+gc.getLeaves().getAutomorphismGroup().maxOrbit());
			        	
			        	fail = true; // skip to next class
			        } 
			        executor.shutdownNow();
			        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
				}
				
				if(!fail && (bench.equals(Benchmark.LABEL) || bench.equals(Benchmark.BOTH))){
					CanonicalLabellingArgs cla = new CanonicalLabellingArgs();
					cla.setHashFunction(hf);
					
					GraphLabelling cl = new GraphLabelling(data,cla);
					
					ExecutorService executor = Executors.newSingleThreadExecutor();
			        Future<CanonicalLabellingResult> future = executor.submit(cl);

			        try {
			        	long b4 = System.currentTimeMillis();
			            LOG.info("Running labelling ...");
			            CanonicalLabellingResult clr = future.get(timeout, TimeUnit.SECONDS);
			            LOG.info("... finished!");
			            
			            System.out.println("LABEL\t"+fn.getName()+"\t"+testClass.getKey()+"\t"+classInstance.getKey()+"\t"+data.size()+"\t"+clr.getBnodeCount()+"\t"+(System.currentTimeMillis()-b4)+"\t"+clr.getColourIterationCount()+"\t"+clr.getLeafCount());
			        } catch (Exception e) {
			        	LOG.info(e.getClass().getName()+" "+e.getMessage());
			        	System.out.println("LABEL\t"+fn.getName()+"\t"+testClass.getKey()+"\t"+classInstance.getKey()+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());//+"\t"+gc.getTotalColourIterations()+"\t"+gc.getLeaves().countLeaves()+"\t"+gc.getLeaves().getAutomorphismGroup().countOrbits()+"\t"+gc.getLeaves().getAutomorphismGroup().maxOrbit());
			        	
			        	fail = true; // skip to next class
			        } 
			        executor.shutdownNow();
			        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
				}
				
		        if(fail) break;
			}
			LOG.info("Finished class "+testClass.getKey());
		}
		LOG.info("Finished testcases. Results in standard out.");
	}
	
	public static int countBnodes(Collection<Node[]> data){
		HashSet<BNode> bnodes = new HashSet<BNode>();
		for(Node[] stmt : data){
			for(Node n:stmt){
				if(n instanceof BNode){
					bnodes.add((BNode)n);
				}
			}
		}
		return bnodes.size();
	}
	
	private static ArrayList<Node[]> loadAndConvert(File fn) throws IOException{
		ArrayList<Node[]> data = new ArrayList<Node[]>();
		
		BufferedReader br = new BufferedReader(new FileReader(fn));
		
		String line = null;
		
		while((line=br.readLine())!=null){
			if(line.startsWith("e")){
				String[] tok = line.split(" ");
				BNode b1 = new BNode("b"+tok[1]);
				BNode b2 = new BNode("b"+tok[2]);
				
				data.add(new Node[]{b1,PRED,b2});
				data.add(new Node[]{b2,PRED,b1});
			}
		}
		
		br.close();
		
		return data;
	}
	
	/**
	 * From file system, build map of test case classes (directory) and size (-num suffix on file)
	 * @param dir
	 * @param suffix
	 * @return
	 * @throws IOException
	 */
	private static TreeMap<String,TreeMap<Integer,File>> buildTestcases(File dir) throws IOException{
		if(!dir.isDirectory()){
			throw new IOException(dir +" is not a directory");
		}
		
		TreeMap<String,TreeMap<Integer,File>> testCases = new  TreeMap<String,TreeMap<Integer,File>>();
		
		TreeMap<Integer,File> classCases = new TreeMap<Integer,File>();
		testCases.put(dir.getName(),classCases);
		
		File[] sub = dir.listFiles();
		
		for(File s:sub){
			if(s.isDirectory()){
				testCases.putAll(buildTestcases(s));
			} else if(!s.getName().endsWith(".zip")){
				String splits[] = s.getName().split("-");
				try{
				int k = Integer.parseInt(splits[splits.length-1]);
					classCases.put(k,s);
				} catch(Exception e){
					//not a test file
				}
			}
		}
		return testCases;
	}

//	private static void expandZip(String fn) throws FileNotFoundException, IOException {
//		BufferedOutputStream dest = null;
//		FileInputStream fis = new FileInputStream(fn);
//		ZipInputStream zis = new ZipInputStream(new BufferedInputStream(fis));
//		ZipEntry entry = null;
//		
//		LOG.info("Unzipping "+fn+" ...");
//		int entries = 0;
//		
//		while((entry = zis.getNextEntry())!= null) {
//			System.out.println("Extracting " +entry);
//			entries++;
//			int count;
//			byte data[] = new byte[BUFFER];
//			// write the files to the disk
//			FileOutputStream fos = new FileOutputStream(entry.getName());
//			dest = new BufferedOutputStream(fos, BUFFER);
//			while((count = zis.read(data, 0, BUFFER)) 
//					!= -1) {
//				dest.write(data, 0, count);
//			}
//			dest.flush();
//			dest.close();
//		}
//		zis.close();
//		
//		LOG.info("... unzipped "+entries+" entries from "+fn+".");
//	}
//
//	public static String downloadZip(String loc, String dir) throws IOException{
//		LOG.info("Downloading zip ...");
//		
//		URL url = new URL(loc);
//		HttpURLConnection con = (HttpURLConnection) url.openConnection();
//
//		// Check for errors
//		int responseCode = con.getResponseCode();
//		InputStream inputStream;
//		if (responseCode == HttpURLConnection.HTTP_OK) {
//			inputStream = con.getInputStream();
//		} else {
//			throw new IOException("Response code "+responseCode+" from "+loc);
//		}
//
//		String file = dir+"/"+ZIP_FN;
//		OutputStream output = new FileOutputStream(dir+"/"+ZIP_FN);
//
//		// Process the response
//		byte[] buffer = new byte[8 * 1024];
//		int bytesRead;
//		while ((bytesRead = inputStream.read(buffer)) > 0) {
//			output.write(buffer, 0, bytesRead);
//		}
//
//		output.close();
//		inputStream.close();
//		
//		LOG.info("... downloaded to "+file+".");
//
//		return file;
//	}
}
