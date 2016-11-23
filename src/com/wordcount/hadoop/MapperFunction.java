package com.wordcount.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapred.*;

public class MapperFunction extends MapReduceBase implements Mapper< LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		IntWritable one = new IntWritable( 1 );
		Text word = new Text();
		String line = value.toString();
		StringTokenizer it = new StringTokenizer( line.toLowerCase() );
		while( it.hasMoreTokens() ) {
			word.set( it.nextToken() );
			output.collect( word, one );
		}
	}
}