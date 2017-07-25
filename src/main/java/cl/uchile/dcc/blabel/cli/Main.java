package cl.uchile.dcc.blabel.cli;

import java.lang.reflect.Method;

/**
 * Class for running one of many possible command line tasks
 * in the CLI package.
 * 
 * @author Aidan Hogan
 */
public class Main {
	
	private static final String PREFIX = Main.class.getPackage().getName()+".";
	private static final String USAGE = "usage: "+Main.class.getName();

	/**
	 * Main method
	 * @param args Command line args, first of which is the utility to run
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 1) {
				StringBuffer sb = new StringBuffer();
				sb.append("missing <utility> arg where <utility> one of");
				sb.append("\n\t"+LabelRDFGraph.class.getSimpleName()+": Run labelling over an RDF graph encoded as N-Triples");
				sb.append("\n\t"+RunNQuadsTest.class.getSimpleName()+": [Testing] Compute the canonical graphs in a quads file");
				sb.append("\n\t"+Control.class.getSimpleName()+": [Testing] Run a control experiment to time parsing a quads file");
				sb.append("\n\t"+RunSyntheticEvaluation.class.getSimpleName()+": [Testing] Run synthetic benchmark");
				/** TODO: Provide documentation for the CLI */
				sb.append("\n\t"+AnalyseNQuadsResults.class.getSimpleName()+": [Testing] ");
				sb.append("\n\t"+UndirectedGraphToRDF.class.getSimpleName()+": [Testing] ");
				
				usage(sb.toString());
			}


			Class<? extends Object> cls = Class.forName(PREFIX + args[0]);

			Method mainMethod = cls.getMethod("main", new Class[] { String[].class });

			String[] mainArgs = new String[args.length - 1];
			System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);

			long time = System.currentTimeMillis();
			
			mainMethod.invoke(null, new Object[] { mainArgs });

			long time1 = System.currentTimeMillis();

			System.err.println("time elapsed " + (time1-time) + " ms");
		} catch (Throwable e) {
			e.printStackTrace();
			usage(e.toString());
		}
	}

	private static void usage(String msg) {
		System.err.println(USAGE);
		System.err.println(msg);
		System.exit(-1);
	}
}
