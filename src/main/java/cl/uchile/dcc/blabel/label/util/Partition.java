package cl.uchile.dcc.blabel.label.util;

import java.util.TreeSet;

/**
 * Maintains a partition using Union-Find.
 * 
 * @author Aidan
 *
 * @param <E>
 */
public class Partition<E> extends MapTreeSet<E,E> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int partitions;
	
	public Partition(){
		super();
	}
	
	public <F> Partition(MapTreeSet<F,E> map){
		super();
		
		for(TreeSet<E> set: map.values()){
			for(E el:set){
				put(el,set);
			}
		}
	}
	
	public TreeSet<E> getPartition(E e){
		return get(e);
	}
	
	public int countPartitions(){
		return partitions; 
	}
	
	public int countElements(){
		return size(); 
	}
	
	public boolean addPair(E a, E b){
		TreeSet<E> ha = get(a);
		TreeSet<E> hb = get(b);
		
		if(ha == null && hb == null){
			ha = new TreeSet<E>();
			partitions++;
			ha.add(a);
			ha.add(b);
			put(a, ha);
			put(b, ha);
		} else if(ha == null){
			hb.add(a);
			put(a,hb);
		} else if(hb==null){
			ha.add(b);
			put(b,ha);
		} else if(ha != hb){
			TreeSet<E> small = null;
			TreeSet<E> big = null;
			
			if(ha.size()>hb.size()){
				small = hb;
				big = ha;
			} else{
				big = hb;
				small = ha;
			}
			
			for(E s: small){
				big.add(s);
				put(s, big);
			}
			partitions--;
		} else return false;
		
		return true;
	}
}
