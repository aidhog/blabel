package cl.uchile.dcc.skolem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A map that maintains an ordered set for duplicate keys rather than
 * overwriting.
 * 
 * @author Aidan
 *
 * @param <E>
 */
public class MapArrayList<E,F> extends HashMap<E,ArrayList<F>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MapArrayList(){
		super();
	}
	
	/**
	 * Will invert the map.
	 */
	public MapArrayList(Map<? extends F,? extends E> toInvert){
		super();
		for(Map.Entry<? extends F, ? extends E> e:toInvert.entrySet()){
			add(e.getValue(),e.getKey());
		}
	}
	
	public boolean add(E a, F b){
		ArrayList<F> set = get(a);
		if(set == null){
			set = new ArrayList<F>();
			put(a,set);
		}
		
		return set.add(b);
	}
}
