package cl.uchile.dcc.blabel.cli;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxBufferedWriter;
import org.semanticweb.yars.util.FlyweightNodeIterator;

import cl.uchile.dcc.blabel.label.GraphLabelling;
import cl.uchile.dcc.blabel.label.GraphLabelling.CanonicalLabellingArgs;
import cl.uchile.dcc.blabel.label.GraphLabelling.CanonicalLabellingResult;
import cl.uchile.dcc.blabel.label.util.GraphLabelIterator;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class LabelRDFGraph {
	static final Logger LOG = Logger.getLogger(LabelRDFGraph.class.getSimpleName());
	public static final Level LOG_LEVEL = Level.INFO;
	static{
		for(Handler h : LOG.getParent().getHandlers()){
		    if(h instanceof ConsoleHandler){
		        h.setLevel(LOG_LEVEL);
		    }
		} 
		LOG.setLevel(LOG_LEVEL);
	}

	public static int FW = 100000;

	public static String STD = "std";

	public static String DEFAULT_ENCODING = "UTF-8";

	public static void main(String[] args) throws IOException{
		long b4 = System.currentTimeMillis();

		Option iO = new Option("i", "input file [enter '"+STD+"' for stdin]");
		iO.setArgs(1);
		iO.setRequired(true);

		Option ieO = new Option("ie", "input encoding [default "+DEFAULT_ENCODING+"]");
		ieO.setArgs(1);

		Option igzO = new Option("igz", "input is GZipped");
		igzO.setArgs(0);

		Option helpO = new Option("h", "print help");

		Option sO = new Option("s", "hashing scheme: 0:md5 1:murmur3_128 2:sha1 3:sha256 4:sha512 (default "+CanonicalLabellingArgs.DEFAULT_HASHING.toString()+")");
		sO.setArgs(1);

		Option bO = new Option("b", "output labels as blank nodes");
		bO.setArgs(0);

		Option pO = new Option("p", "string prefix to append to label [make sure it's valid for URI or blank node!] [default empty string])");
		pO.setArgs(1);

		Option ddpO = new Option("ddp", "don't distinguish partitions [isomorphic blank node partitions will be removed; by default they are distinguished and kept]");
		ddpO.setArgs(0);

		Option uppO = new Option("upp", "keep blank nodes unique per partition, not graph [blank nodes are labelled only using information from the partition; by default the entire graph is encoded in the blank node label including ground triples]");
		uppO.setArgs(0);

		Option oO = new Option("o", "output file [enter '"+STD+"' for stdout]");
		oO.setArgs(1);
		oO.setRequired(true);

		Option ogzO = new Option("ogz", "output should be GZipped");
		ogzO.setArgs(0);

		Option oeO = new Option("oe", "output encoding [default "+DEFAULT_ENCODING+"]");
		oeO.setArgs(1);

		Options options = new Options();
		options.addOption(iO);
		options.addOption(igzO);
		options.addOption(sO);
		options.addOption(helpO);
		options.addOption(oO);
		options.addOption(ogzO);
		options.addOption(pO);
		options.addOption(bO);
		options.addOption(ddpO);
		options.addOption(uppO);

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
		if (cmd.hasOption(helpO.getOpt())) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		HashFunction hf = null;

		if(cmd.getOptionValue(sO.getOpt())!=null){
			int s = Integer.parseInt(cmd.getOptionValue(sO.getOpt()));
			switch(s){
			case 0: hf = Hashing.md5(); break;
			case 1: hf = Hashing.murmur3_128(); break;
			case 2: hf = Hashing.sha1(); break;
			case 3: hf = Hashing.sha256(); break;
			case 4: hf = Hashing.sha512(); break;
			}
		}

		InputStream is = null;

		String istr = cmd.getOptionValue(iO.getOpt());
		if(istr.equals(STD)){
			is = System.in;
		} else{
			is = new FileInputStream(istr);
		}
		if(cmd.hasOption(igzO.getOpt())){
			is = new GZIPInputStream(is);
		}

		String iestr = cmd.getOptionValue(ieO.getOpt());
		if(iestr==null){
			iestr = DEFAULT_ENCODING;
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(is,iestr));
		NxParser nxp = new NxParser(br);

		if(!nxp.hasNext()){
			LOG.info("Empty input");
			return;
		}


		OutputStream os = null;
		String ostr = cmd.getOptionValue(oO.getOpt());
		if(ostr.equals(STD)){
			os = System.out;
		} else{
			os = new FileOutputStream(ostr);
		}
		if(cmd.hasOption(ogzO.getOpt())){
			os = new GZIPOutputStream(os);
		}

		String oestr = cmd.getOptionValue(oeO.getOpt());
		if(oestr==null){
			oestr = DEFAULT_ENCODING;
		}

		boolean writeBnode = cmd.hasOption(bO.getOpt());

		String prefix = "";
		if(cmd.hasOption(pO.getOpt())){
			prefix = cmd.getOptionValue(pO.getOpt());
		}

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,oestr));
		CallbackNxBufferedWriter cb = new CallbackNxBufferedWriter(bw);

		LOG.info("Starting process ...");
		// set the arguments for the labelling
		CanonicalLabellingArgs cla = new CanonicalLabellingArgs();
		if(hf!=null)
			cla.setHashFunction(hf);

		cla.setDistinguishIsoPartitions(!cmd.hasOption(ddpO.getOpt()));
		cla.setUniquePerGraph(!cmd.hasOption(uppO.getOpt()));

		// re-use node references: saves mem
		// at cost of map lookups
		Iterator<Node[]> iter = nxp;
		iter = new FlyweightNodeIterator(FW,iter);

		// does the main work
		labelGraph(iter,cb,cla,prefix,writeBnode);

		LOG.info("Finished in "+(System.currentTimeMillis()-b4)+" ms");
		br.close();
		bw.close();
	}

	/**
	 * Labels the input graph and writes the result to the callback.
	 * 
	 * @param in - The input data in Nx format
	 * @param out - The output data in Nx format
	 * @param cla - The options for running the labelling
	 * @param prefix - Any prefix to be prepended to the label (e.g., a skolem prefix)
	 * @param writeBnode - Writes bnodes if true, otherwise writes URIs
	 * 
	 * @returns null if no blank nodes in graph, otherwise returns an object with the details of the colouring process (including, e.g., a unique hash) 
	 */
	public static final CanonicalLabellingResult labelGraph(Iterator<Node[]> in, Callback out, CanonicalLabellingArgs cla, String prefix, boolean writeBnode){
		// load the graph into memory
		ArrayList<Node[]> stmts = new ArrayList<Node[]>();
		boolean bnode = false;
		while(in.hasNext()){
			Node[] triple = in.next();
			if(triple.length>=3){
				stmts.add(new Node[]{triple[0], triple[1], triple[2]});
				bnode = bnode | (triple[0] instanceof BNode) | (triple[2] instanceof BNode);
			} else{
				LOG.warning("Not a triple "+Nodes.toN3(triple));
			}
		}
		LOG.info("Loaded "+stmts.size()+" triples");

		CanonicalLabellingResult clr = null;

		if(!bnode){
			LOG.info("No bnodes ... buffering triple input to output");
			for(Node[] triple:stmts){
				out.processStatement(triple);
			}
		} else{
			// create a new labeler
			GraphLabelling cl = new GraphLabelling(stmts,cla);

			try{
				LOG.info("Running labelling ...");
				clr = cl.call();
				LOG.info("... done.");

				LOG.info("Number of blank nodes: "+clr.getBnodeCount());
				LOG.info("Number of partitions: "+clr.getPartitionCount());
				LOG.info("Number of colour iterations: "+clr.getColourIterationCount());
				LOG.info("Number of leafs: "+clr.getLeafCount());
				LOG.info("Graph hash: "+clr.getHashGraph().getGraphHash());

				// the canonical labeling writes blank node using hashes w/o prefix
				// this code adds the prefix and maps them to URIs or blank nodes
				// as specified in the options
				LOG.info("Writing output ...");
				int written = 0;
				TreeSet<Node[]> canonicalGraph = clr.getGraph();
				GraphLabelIterator gli = new GraphLabelIterator(canonicalGraph.iterator(), prefix, writeBnode);
				while(gli.hasNext()){
					out.processStatement(gli.next());
					written ++;
				}
				LOG.info("... written "+written+" statements.");
			} catch(Exception e){
				LOG.severe(e.getMessage());
				e.printStackTrace();
			}

		}
		return clr;
	}

}
