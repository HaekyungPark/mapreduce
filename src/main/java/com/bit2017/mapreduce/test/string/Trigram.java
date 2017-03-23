package com.bit2017.mapreduce.test.string;

import java.util.StringTokenizer;

public class Trigram {
	public static void main(String[] args) {
	
	}
	
	public static void trigram(String s){
		StringTokenizer tokenizer = new StringTokenizer(s, "\r\n\t,|()<> ''.:");
		
		String firstWord = tokenizer.nextToken();
		String secondWord = tokenizer.nextToken();
		while(tokenizer.hasMoreTokens()){
			String thirdWord = tokenizer.nextToken();
			
			String trigram = firstWord + "" + secondWord + "" + thirdWord;
			
		}
	}
}
