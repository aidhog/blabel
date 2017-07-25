package cl.uchile.dcc.blabel.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
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

public class Control {
	static Logger log = Logger.getLogger(Control.class.getSimpleName());
	
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
		
		Options options = new Options();
		options.addOption(inO);
		options.addOption(ingzO);
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
		
		InputStream is = new FileInputStream(cmd.getOptionValue("i"));
		if(cmd.hasOption("igz"))
			is = new GZIPInputStream(is);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		NxParser nxp = new NxParser(br);
		
		if(!nxp.hasNext()){
			log.info("Empty input");
			return;
		}
		
		Node old = null;
		Node[] stmt = null;
		boolean done = false; 
		
		int doc = 0;
		long read = 0;
		
		log.info("Starting process ...");
		
		Iterator<Node[]> iter = nxp;
		
		// re-use node references: pushes down mem
		// at cost of map lookups
		iter = new FlyweightNodeIterator(FW,iter);
		
		ArrayList<Node[]> stmts = new ArrayList<Node[]>();
		
		boolean bnode = false;
		
		while(!done){
			done = !iter.hasNext();
			if(!done){
				stmt = iter.next();
				read++;
				
				if(read%TICKS==0){
					log.info("Read "+read+" input statements and "+doc+" documents");
				}
			}
			
			if(done || (old!=null && !stmt[3].equals(old))){
				doc++;
				System.out.println(old+"\t"+stmts.size());
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
		
		log.info("Finished! Read "+read+" input statements and "+doc+" documents.");
		
		System.out.println("===============================================");
		System.out.println("Number of statements read:\t"+read);
		System.out.println("===============================================");
		System.out.println("Number of documents read:\t"+doc);
		System.out.println("===============================================");
		System.out.println("Total duration:\t"+(System.currentTimeMillis()-b4));
		System.out.println("===============================================");
		
		br.close();
		
		log.info("Finished in "+(System.currentTimeMillis()-b4));
	}
}
