package cl.uchile.dcc.blabel.lean.util;

import org.semanticweb.yars.nx.BNode;

public class VariableSelectivityEstimate implements Comparable<VariableSelectivityEstimate> {
	private BNode variable;
	private int card;
	
	public VariableSelectivityEstimate(BNode var, int card){
		this.variable = var;
		this.card = card;
	}

	/**
	 * Will update the cardinality if lower
	 * @param card
	 * @return
	 */
	public int updateCardinality(int card){
		if(card<this.card)
			this.card = card;
		return card;
	}

	@Override
	public int compareTo(VariableSelectivityEstimate arg0) {
		int comp = card - arg0.card;
		if(comp!=0)
			return comp;
		return variable.compareTo(arg0.variable);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + card;
		result = prime * result + ((variable == null) ? 0 : variable.hashCode());
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
		VariableSelectivityEstimate other = (VariableSelectivityEstimate) obj;
		if (card != other.card)
			return false;
		if (variable == null) {
			if (other.variable != null)
				return false;
		} else if (!variable.equals(other.variable))
			return false;
		return true;
	}
	
	public String toString(){
		return variable+" ["+card+"]";
	}

	public BNode getVariable() {
		return variable;
	}

	public int getCard() {
		return card;
	}
}
