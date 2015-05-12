package cl.uchile.dcc.skolem.cli;

import java.util.ArrayList;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

public class Scratch {
	public static void main(String[] args){
		HashCode a1 = HashCode.fromString("ec5d7e74e2d75e2166ed4cb95d650b7f");
		HashCode a2 = HashCode.fromString("dc5956e412d3560d9e912c2559b1173b");
		
		HashCode b1 = HashCode.fromString("ec5d7e74e2d75e2166ed4cb9dd658b7f");
		HashCode b2 = HashCode.fromString("dc5956e412d3560d9e912c2559b1173b");
		
		HashCode blank = HashCode.fromString("d41d8cd98f00b204e9800998ecf8427e");
		
		
		ArrayList<HashCode> tup = new ArrayList<HashCode>();
		tup.add(a2);
		tup.add(blank);
		tup.add(a1);
		System.err.println(Hashing.combineOrdered(tup));
		
		tup.clear();
		tup.add(b2);
		tup.add(blank);
		tup.add(b1);
		System.err.println(Hashing.combineOrdered(tup));
	}
}
