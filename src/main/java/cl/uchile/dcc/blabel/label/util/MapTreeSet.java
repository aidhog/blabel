package cl.uchile.dcc.blabel.label.util;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * A map that maintains an ordered set for duplicate keys rather than
 * overwriting.
 * 
 * @author Aidan
 *
 * @param <E>
 */
public class MapTreeSet<E,F> extends HashMap<E,TreeSet<F>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MapTreeSet(){
		super();
	}
	
	/**
	 * Will invert the map.
	 */
	public MapTreeSet(Map<? extends F,? extends E> toInvert){
		super();
		for(Map.Entry<? extends F, ? extends E> e:toInvert.entrySet()){
			add(e.getValue(),e.getKey());
		}
	}
	
	public boolean add(E a, F b){
		TreeSet<F> set = get(a);
		if(set == null){
			set = new TreeSet<F>();
			put(a,set);
		}
		
		return set.add(b);
	}
}
