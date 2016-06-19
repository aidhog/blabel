package cl.uchile.dcc.blabel.label.util;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;

import cl.uchile.dcc.blabel.label.GraphColouring;

public class Leaves extends TreeMap<TreeSet<Node[]>,ArrayList<GraphColouring>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7387294009806260753L;
	
	int leavesExplored = 0;
	
	public Leaves(){
		super(GraphColouring.GRAPH_COMP);
	}
	
	public boolean add(TreeSet<Node[]> graph, GraphColouring gc){
		ArrayList<GraphColouring> agc = this.get(graph);
		if(agc==null){
			agc = new ArrayList<GraphColouring>();
			put(graph,agc);
		}
		leavesExplored++;
		return agc.add(gc);
	}
	
	public int countLeaves(){
		return leavesExplored;
	}
}
