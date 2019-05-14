package com.virtualpairprogrammers;

public class IntegerWithSquareRoot {

	private int originalInteger;
	private double squareRoot;
	
	public IntegerWithSquareRoot(int originalInteger) {
		this.originalInteger = originalInteger;
		this.squareRoot = Math.sqrt(originalInteger);
	}

}
