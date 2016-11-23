package com.wordcount.hadoop;

class WordFrequencyPair implements Comparable {
	String word;
	int frequency;
	WordFrequencyPair( String word, int frequency ) {
		this.word = word;
		this.frequency = frequency;
		return;
	}
	
	@Override
	public int compareTo( Object B ) {
		WordFrequencyPair other = ( WordFrequencyPair )( B );
		return this.frequency - other.frequency;
	}
}