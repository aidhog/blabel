package cl.uchile.dcc.blabel.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.semanticweb.yars.stats.Count;

public class AnalyseNQuadsResults {
	static Logger log = Logger.getLogger(AnalyseNQuadsResults.class.getSimpleName());

	public static int TICKS = 10000000;
	public static int FW = 100000;

	public static long[][] DEFAULT_BINS = new long[][]{
		{ 0l, 9l} , { 10l, 99l} , { 100l, 999l} , { 1000l, 9999l} ,  { 10000l, 99999l} , { 100000l, 1000000l} 
	};

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

		String line;

		long totalLeanInputTriples = 0;
		long totalLeanOutputTriples = 0;
		long totalLeanErrorTriples = 0;

		long totalLeanInputBnodes = 0;	
		long totalLeanOutputBnodes = 0;
		long totalLeanErrorBnodes = 0;
		
		long leanNonLeanInputBnodes = 0;	
		long leanNonLeanOutputBnodes = 0;
		
		long leanNonLeanInputTriples = 0;	
		long leanNonLeanOutputTriples = 0;

		long totalLabelInputTriples = 0;
		long totalLabelErrorTriples = 0;

		long totalLabelInputBnodes = 0;
		long totalLabelErrorBnodes = 0;

		long leanCount = 0;
		long labelCount = 0;
		long nobnodeCount = 0;

		long nonLeanCount = 0;

		long leanErrorCount = 0;
		long labelErrorCount = 0;

		long errorLeanTime = 0;
		long errorLabelTime = 0;

		long noErrorLeanTime = 0;
		long noErrorLabelTime = 0;

		long maxTriples = 0;
		String maxTriplesDoc = null;
		long maxTriplesBnodes = 0;

		long maxBnodes = 0;
		String maxBnodesDoc = null;
		long maxBnodesTriples = 0;
		
		long maxLeanedBnodes = 0;
		String maxLeanedBnodesDoc = null;
		long maxLeanedBnodesIn = 0;
		long maxLeanedBnodesOut = 0;
		long maxLeanedBnodesTriplesIn = 0;
		long maxLeanedBnodesTriplesOut = 0;
		
		long maxLeanedTriples = 0;
		String maxLeanedTriplesDoc = null;
		long maxLeanedTriplesIn = 0;
		long maxLeanedTriplesOut = 0;
		long maxLeanedTriplesBnodesIn = 0;
		long maxLeanedTriplesBnodesOut = 0;
		
		int lastRuntime = 0;
		int runtime = 0;

		Binner leanTimeBins = new Binner(DEFAULT_BINS);
		Binner labelTimeBins = new Binner(DEFAULT_BINS);
		Binner combinedTimeBins = new Binner(DEFAULT_BINS);

		Count<String> exceptionTypeLean = new Count<String>();
		Count<String> exceptionTypeLabel = new Count<String>();

		while((line = br.readLine())!=null){
			try{
				boolean lean = line.startsWith("LEAN");
				boolean label = !lean && line.startsWith("LABEL");
				boolean nobnode = !lean && !label && line.startsWith("NOBNODES");
				// System.out.println("LEAN\t"+old+"\t"+"\t"+data.size()+"\t"+bnodeCount+"\t"+runtime+"\t"+glr.getLeanData().size()+"\t"+leanBnodeCount+"\t"+glr.getJoins()+"\t"+glr.getDepth()+"\t"+glr.getSolutionCount()+"\t"+(data.size()-glr.getLeanData().size())+"\t"+(bnodeCount-leanBnodeCount));
				// System.out.println("LEAN\t"+old+"\t"+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());
				// System.out.println("LABEL\t"+old+"\t"+"\t"+data.size()+"\t"+clr.getBnodeCount()+"\t"+runtime+"\t"+clr.getColourIterationCount()+"\t"+clr.getLeafCount());
				// System.out.println("LABEL\t"+old+"\t"+data.size()+"\t"+bnodeCount+"\t"+(-1*timeout*1000)+"\t"+e.getClass().getSimpleName());

				if(lean){
					leanCount++;
				} else if(label){
					labelCount++;
				} else if(nobnode){
					nobnodeCount++;
				}


				if(lean || label){	
					line = line.replace("\t\t", "\t"); //workaround for a bug?
					String[] split = line.split("\t");
					String doc = split[1];

					int triples = Integer.parseInt(split[2]);
					int bnodes = Integer.parseInt(split[3]);
					runtime = Integer.parseInt(split[4]);

					if(triples>maxTriples){
						maxTriples = triples;
						maxTriplesDoc = doc;
						maxTriplesBnodes = bnodes;
					}

					if(bnodes>maxBnodes){
						maxBnodes = bnodes;
						maxBnodesDoc = doc;
						maxBnodesTriples = triples;
					}

					boolean error = split.length < 8;

					if(error){
						if(lean){
							exceptionTypeLean.add(split[split.length-1]);
							leanErrorCount++;
							errorLeanTime += Math.abs(runtime);
							totalLeanErrorTriples += triples;
							totalLeanErrorBnodes += bnodes;

						}
						if(label){
							exceptionTypeLabel.add(split[split.length-1]);
							labelErrorCount++;
							errorLabelTime += Math.abs(runtime);
							totalLabelErrorTriples += triples;
							totalLabelErrorBnodes += bnodes;
						}
					} else {
						if(lean){
							int leanTriples = Integer.parseInt(split[5]);
							int leanBnodes = Integer.parseInt(split[6]);
							//						int joins = Integer.parseInt(split[7]);
							//						int depth = Integer.parseInt(split[8]);
							//						int sols = Integer.parseInt(split[9]);

							noErrorLeanTime += runtime;

							leanTimeBins.add(runtime);

							if(leanTriples != triples){
								nonLeanCount ++;
								
								leanNonLeanInputTriples +=triples;
								leanNonLeanOutputTriples +=leanTriples;
								
								leanNonLeanInputBnodes +=bnodes;
								leanNonLeanOutputBnodes +=leanBnodes;
								
								int leanedBnodes = bnodes - leanBnodes;
								
								if(leanedBnodes>maxLeanedBnodes){
									maxLeanedBnodes = leanedBnodes;
									maxLeanedBnodesDoc = doc;
									maxLeanedBnodesIn = bnodes;
									maxLeanedBnodesOut = leanBnodes;
									maxLeanedBnodesTriplesIn = triples;
									maxLeanedBnodesTriplesOut = leanTriples;
								}
								
								int leanedTriples = triples - leanTriples;
								
								if(leanedTriples>maxLeanedTriples){
									maxLeanedTriples = leanedTriples;
									maxLeanedTriplesDoc = doc;
									maxLeanedTriplesIn = triples;
									maxLeanedTriplesOut = leanTriples;
									maxLeanedTriplesBnodesIn = bnodes;
									maxLeanedTriplesBnodesOut = leanBnodes;
									
								}
							}

							totalLeanInputTriples += triples;
							totalLeanInputBnodes += bnodes;

							totalLeanOutputTriples += leanTriples;
							totalLeanOutputBnodes += leanBnodes;
						}
						if(label){
							//						int colIters = Integer.parseInt(split[4]);
							//						int leaves = Integer.parseInt(split[5]);

							noErrorLabelTime += runtime;

							labelTimeBins.add(runtime);
							
							if(leanCount>0){
								combinedTimeBins.add(runtime+lastRuntime);
							}

							totalLabelInputTriples += triples;
							totalLabelInputBnodes += bnodes;
						}
					}
				}
			} catch(Exception e){
				System.err.println("Have to skip line "+line);
				e.printStackTrace();
			} finally{
				lastRuntime = runtime;
			}
		}

		br.close();

		System.out.println("Graphs with no bnodes "+nobnodeCount);
		System.out.println("Largest graph "+maxTriplesDoc+" triples:"+maxTriples+" bnodes:"+maxTriplesBnodes);
		System.out.println("Graph with most bnodes "+maxBnodesDoc+" triples:"+maxBnodesTriples+" bnodes:"+maxBnodes);
		System.out.println();
		System.out.println("Graphs input for leaning "+leanCount);
		System.out.println("Graphs failed for leaning "+leanErrorCount);
		System.out.println("Graphs found to be non-lean "+nonLeanCount);
		System.out.println("Total triples in input non-lean graphs "+leanNonLeanInputTriples);
		System.out.println("Total blank nodes in input non-lean graphs "+leanNonLeanInputBnodes);
		System.out.println("Total triples in output non-lean graphs "+leanNonLeanOutputTriples);
		System.out.println("Total blank nodes in output non-lean graphs "+leanNonLeanOutputBnodes);
		System.out.println("Total leaning error input triples "+totalLeanErrorTriples);
		System.out.println("Total leaning error input blank nodes "+totalLeanErrorBnodes);
		System.out.println("Total leaning non-error input triples "+totalLeanInputTriples);
		System.out.println("Total leaning non-error input blank nodes "+totalLeanInputBnodes);
		System.out.println("Total leaning non-error output triples "+totalLeanOutputTriples);
		System.out.println("Total leaning non-error output blank nodes "+totalLeanOutputBnodes);

		System.out.println("Most leaned graph (per triples) "+maxLeanedTriplesDoc+" input triples:"+maxLeanedTriplesIn+" output triples:"+maxLeanedTriplesOut+" input bnodes:"+maxLeanedTriplesBnodesIn+" output bnodes:"+maxLeanedTriplesBnodesOut);
		System.out.println("Most leaned graph (per bnodes) "+maxLeanedBnodesDoc+" input triples:"+maxLeanedBnodesTriplesIn+" output triples:"+maxLeanedBnodesTriplesOut+" input bnodes:"+maxLeanedBnodesIn+" output bnodes:"+maxLeanedBnodesOut);

		System.out.println("Total leaning time "+(errorLeanTime+noErrorLeanTime));
		System.out.println("Non-error leaning time "+(noErrorLeanTime));
		System.out.println("Error leaning time "+(errorLeanTime));
		System.out.println("Leaning runtime bins");
		System.out.println(leanTimeBins.toString());
		System.out.println("Leaning error counts");
		exceptionTypeLean.printOrderedStats();
		System.out.println();

		System.out.println("Graphs input for labelling "+labelCount);
		System.out.println("Graphs failed for labelling "+labelErrorCount);
		System.out.println("Total labelling time "+(errorLabelTime+noErrorLabelTime));
		System.out.println("Non-error labelling time "+(noErrorLabelTime));
		System.out.println("Error labelling time "+(errorLabelTime));
		System.out.println("Total labelling error input triples "+totalLabelErrorTriples);
		System.out.println("Total labelling error input blank nodes "+totalLabelErrorBnodes);
		System.out.println("Total labelling non-error input triples "+totalLabelInputTriples);
		System.out.println("Total labelling non-error input blank nodes "+totalLabelInputBnodes);
		System.out.println("Labelling runtime bins");
		System.out.println(labelTimeBins.toString());
		System.out.println("Labelling error counts");
		exceptionTypeLabel.printOrderedStats();
		System.out.println();
		
		System.out.println("Combined runtime bins");
		System.out.println(combinedTimeBins.toString());

		log.info("Finished in "+(System.currentTimeMillis()-b4));
	}

	public static class Binner {
		final long[] counts;
		final long[][] bins;

		Binner(long[][] bins){
			counts = new long[bins.length];
			this.bins = bins;
		}

		void add(long val){
			for(int i=0; i<bins.length; i++){
				long[] bin = bins[i];
				if(val>=bin[0] && val<=bin[1]){
					counts[i]++;
				}
			}
		}

		public String toString(){
			StringBuilder sb = new StringBuilder();
			for(long[] bin:bins){
				sb.append(bin[0]+"-"+bin[1]+"\t");
			}
			sb.append("\n");
			for(long count:counts){
				sb.append(count+"\t");
			}

			return sb.toString();
		}
	}
}
