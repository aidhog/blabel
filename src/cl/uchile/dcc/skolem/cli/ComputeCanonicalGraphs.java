package cl.uchile.dcc.skolem.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
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
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.util.FlyweightNodeIterator;

import cl.uchile.dcc.skolem.CanonicalLabelling;
import cl.uchile.dcc.skolem.CanonicalLabelling.CanonicalLabellingArgs;
import cl.uchile.dcc.skolem.CanonicalLabelling.CanonicalLabellingResult;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ComputeCanonicalGraphs {
	static Logger LOG = Logger.getLogger(ComputeCanonicalGraphs.class.getSimpleName());
	
	public static int TICKS = 10000000;
	public static int FW = 100000;
	
	public static void main(String[] args) throws IOException{
		long b4 = System.currentTimeMillis();
		
		Option inO = new Option("i", "input file");
		inO.setArgs(1);
		inO.setRequired(true);
		
		Option ingzO = new Option("igz", "input file is GZipped");
		ingzO.setArgs(0);
		
		Option helpO = new Option("h", "print help");
		
		Option sO = new Option("s", "hashing scheme: 0:md5 1:murmur3_128 2:sha1 3:sha256 4:sha512");
		sO.setArgs(1);
				
		Options options = new Options();
		options.addOption(inO);
		options.addOption(ingzO);
		options.addOption(sO);
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
		
		HashFunction hf = null;
		
		int s = Integer.parseInt(cmd.getOptionValue("s"));
		switch(s){
			case 0: hf = Hashing.md5(); break;
			case 1: hf = Hashing.murmur3_128(); break;
			case 2: hf = Hashing.sha1(); break;
			case 3: hf = Hashing.sha256(); break;
			case 4: hf = Hashing.sha512(); break;
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
		
		Count<Integer> numberBnodes = new Count<Integer>();
		Count<Integer> numberParts = new Count<Integer>();
		Count<Integer> numberTriples = new Count<Integer>();
		Count<Integer> numberIters = new Count<Integer>();
		Count<Integer> numberLeaves = new Count<Integer>();
		Map<HashCode,TreeSet<Node>> dupeGraphs = new HashMap<HashCode,TreeSet<Node>>();
		
		long slowestTime = 0;
		Node slowestGraph = null;
		
		int failed = 0;
		int doc = 0;
		long read = 0;
		
		LOG.info("Starting process ...");
		
		Iterator<Node[]> iter = nxp;
		
		// re-use node references: pushes down mem
		// at cost of map lookups
		iter = new FlyweightNodeIterator(FW,iter);
		
		ArrayList<Node[]> stmts = new ArrayList<Node[]>();
		
		boolean bnode = false;
		
		CanonicalLabellingArgs cla = new CanonicalLabellingArgs();
		cla.setHashFunction(hf);
		
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
				numberTriples.add(stmts.size());
				
				if(!bnode){
					// no blank nodes ... nothing to do
					
					numberParts.add(0);
					System.out.println(old+"\t"+stmts.size()+"0\tNOBNODES");
				} else{
					// found blank nodes, time to compute canonical labelling
					long b4G = System.currentTimeMillis();
					CanonicalLabelling cl = new CanonicalLabelling(stmts,cla);
					
					try{
						CanonicalLabellingResult clr = cl.call();
						numberBnodes.add(clr.getBnodeCount());
						numberParts.add(clr.getPartitionCount());
						numberIters.add(clr.getColourIterationCount());
						numberLeaves.add(clr.getLeafCount());
						
						HashCode hc = clr.getHashGraph().getGraphHash();
						TreeSet<Node> dupes = dupeGraphs.get(hc);
						
						if(dupes==null){
							dupes = new TreeSet<Node>();
							dupeGraphs.put(hc, dupes);
						}
						
						dupes.add(old);
						
						System.out.print(old+"\t"+stmts.size()+"\t"+clr.getBnodeCount()+"\t"+clr.getPartitionCount()
								+"\t"+clr.getColourIterationCount()+"\t"+clr.getLeafCount());
					} catch(Exception e){
						LOG.info(e.getMessage());
						e.printStackTrace();
						failed ++;
						System.out.print(old+"\tFAILED!");
					}
					
						
					long afG = System.currentTimeMillis();
						
					long duration = afG-b4G;
					System.out.println("\t"+duration);
					
					if(duration>slowestTime){
						slowestTime = duration;
						slowestGraph = old;
					}
				}
				stmts.clear();
				bnode = false;
			} 

			if(!done){
				stmts.add(stmt);
				old = stmt[3];
				if(!bnode && (stmt[0] instanceof BNode || stmt[2] instanceof BNode)){
					bnode = true;
				}
			}
			
		}
		
		LOG.info("Finished! Read "+read+" input statements and "+doc+" documents.");
		
		LOG.info("Sorting duplicate graphs by size of class ...");
		TreeSet<TreeSet<Node>> dupes = new TreeSet<TreeSet<Node>>(new BiggestTreeSetComparator());
		dupes.addAll(dupeGraphs.values());
		LOG.info("... done.");
		
		System.out.println("===============================================");
		System.out.println("Duplicate graphs");
		for(TreeSet<Node> dupe:dupes){
			if(dupe.size()>1){
				System.out.println(dupe);
			}
		}
		
		System.out.println("===============================================");
		System.out.println("Distribution of counts of blank nodes:");
		numberBnodes.printOrderedStats(System.out);
		System.out.println("===============================================");
		System.out.println("Distribution of counts of blank node partitions:");
		numberParts.printOrderedStats(System.out);
		System.out.println("===============================================");
		System.out.println("Distribution of counts of number of triples:");
		numberTriples.printOrderedStats(System.out);
		System.out.println("===============================================");
		System.out.println("Distribution of counts of number of total colour iterations:");
		numberIters.printOrderedStats(System.out);
		System.out.println("===============================================");
		System.out.println("Distribution of counts of number of leaves:");
		numberLeaves.printOrderedStats(System.out);
		System.out.println("===============================================");
		System.out.println("Number of statements read:\t"+read);
		System.out.println("===============================================");
		System.out.println("Number of documents read:\t"+doc);
		System.out.println("===============================================");
		System.out.println("Number of unique documents read:\t"+dupes.size());
		System.out.println("===============================================");
		System.out.println("Largest collection of isomorphic documents:\t"+dupes.first().size());
		System.out.println("===============================================");
		System.out.println("Number of failures:\t"+failed);
		System.out.println("===============================================");
		System.out.println("Slowest time:\t"+slowestTime);
		System.out.println("===============================================");
		System.out.println("Slowest graph:\t"+slowestGraph);
		System.out.println("===============================================");
		System.out.println("Total duration:\t"+(System.currentTimeMillis()-b4));
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
