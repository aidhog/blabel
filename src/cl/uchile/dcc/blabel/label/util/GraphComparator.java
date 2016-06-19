package cl.uchile.dcc.blabel.label.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;

/**
 * Compares two RDF graphs based on size first and lexical ordering of
 * triples second.
 * 
 * @author Aidan
 *
 */
public class GraphComparator implements Comparator<TreeSet<Node[]>> {
	Comparator<Node[]> c;
	
	public GraphComparator(Comparator<Node[]> c){
		this.c = c;
	}
	
	public int compare(TreeSet<Node[]> o1, TreeSet<Node[]> o2) {
		int diff = o1.size() - o2.size();
		if(diff!=0) return diff;
		
		Iterator<Node[]> i1 = o1.iterator();
		Iterator<Node[]> i2 = o2.iterator();
		
		while(i1.hasNext()){
			Node[] a1 = i1.next();
			Node[] a2 = i2.next();
			diff = c.compare(a1, a2);
			if(diff!=0)
				return diff;
		}
		
		return 0;
	}

}
