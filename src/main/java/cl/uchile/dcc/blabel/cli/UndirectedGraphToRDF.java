package cl.uchile.dcc.blabel.cli;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.util.CallbackNxBufferedWriter;

/**
 * Convenience method to convert from bliss
 * format (undirected graphs) to RDF.
 * 
 * @author ahogan
 *
 */
public class UndirectedGraphToRDF {
	static final Logger LOG = Logger.getLogger(UndirectedGraphToRDF.class.getSimpleName());
	public static final Level LOG_LEVEL = Level.INFO;
	static{
		for(Handler h : LOG.getParent().getHandlers()){
		    if(h instanceof ConsoleHandler){
		        h.setLevel(LOG_LEVEL);
		    }
		} 
		LOG.setLevel(LOG_LEVEL);
	}

	public static String DEFAULT_ENCODING = "UTF-8";

	public static void main(String[] args) throws IOException{
		Option iO = new Option("i", "input file");
		iO.setArgs(1);
		iO.setRequired(true);

		Option helpO = new Option("h", "print help");

		Option oO = new Option("o", "output file");
		oO.setArgs(1);
		oO.setRequired(true);

		Options options = new Options();
		options.addOption(iO);
		options.addOption(helpO);
		options.addOption(oO);

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

		String istr = cmd.getOptionValue(iO.getOpt());
		ArrayList<Node[]> rdf = RunSyntheticEvaluation.loadAndConvert(new File(istr));
		
		String ostr = cmd.getOptionValue(oO.getOpt());
		OutputStream os = new FileOutputStream(ostr);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os,DEFAULT_ENCODING));
		CallbackNxBufferedWriter cb = new CallbackNxBufferedWriter(bw);
		

		LOG.info("Writing "+rdf.size()+" triples ...");
		
		for(Node[] triple:rdf){
			cb.processStatement(triple);
		}
		LOG.info("... done");
		
		cb.endDocument();
		os.close();	
	}

}
