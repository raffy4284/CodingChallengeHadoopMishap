package com.wordcount.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class ReducerFunction extends MapReduceBase implements Reducer< Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce( Text key, Iterator value, OutputCollector output, Reporter reporter ) throws IOException {
		int sum = 0;
		while( value.hasNext() ) {
			sum += ( ( IntWritable ) value.next() ).get();
		}
		output.collect( key, new IntWritable( sum ) );
	}
}