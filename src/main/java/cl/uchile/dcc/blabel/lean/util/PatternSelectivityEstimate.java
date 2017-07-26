package cl.uchile.dcc.blabel.lean.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;

public class PatternSelectivityEstimate implements Comparable<PatternSelectivityEstimate>{
	private Node[] pattern; 
	private int[] card;
	private ArrayList<Integer> cardList;
	
	public PatternSelectivityEstimate(Node[] pattern, int... card){
		this.pattern = pattern;
		this.card = card;
		cardList = new ArrayList<Integer>();
		for(int c: card)
			cardList.add(c);
		Collections.sort(cardList);
	}

	@Override
	public int compareTo(PatternSelectivityEstimate arg0) {
		Iterator<Integer> thisIter = cardList.iterator();
		Iterator<Integer> thatIter = arg0.cardList.iterator();
		while(thisIter.hasNext() && thatIter.hasNext()){
			int comp = thisIter.next() - thatIter.next();
			if(comp!=0) return comp;
		}
		for(int i=0; i<cardList.size()&&i<arg0.cardList.size(); i++){
			int comp = cardList.get(i) - arg0.cardList.get(i);
			if(comp!=0) return comp;
		}
		
		int comp = cardList.size() - arg0.cardList.size();
		if(comp!=0) return comp;
		
		return NodeComparator.NC.compare(pattern, arg0.pattern);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cardList == null) ? 0 : cardList.hashCode());
		result = prime * result + Arrays.hashCode(pattern);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PatternSelectivityEstimate other = (PatternSelectivityEstimate) obj;
		if (cardList == null) {
			if (other.cardList != null)
				return false;
		} else if (!cardList.equals(other.cardList))
			return false;
		if (!Arrays.equals(pattern, other.pattern))
			return false;
		return true;
	}

	@Override
	public String toString(){
		return Nodes.toN3(pattern) + "card: " + Arrays.toString(card) + "cardList: " + cardList;
	}

	public Node[] getPattern() {
		return pattern;
	}

	public int[] getCard() {
		return card;
	}

	public ArrayList<Integer> getCardList() {
		return cardList;
	}
}
