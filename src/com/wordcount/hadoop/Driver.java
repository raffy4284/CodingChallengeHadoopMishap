package com.wordcount.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/*	How to use:
 * 	hadoop jar build/jar/WordCount.jar <K> <input1>, <input2>, <input3>...
 *  It is assumed that you have hadoop running
 *  
 */

/* 	
 *	result will essentially be a dictionary of text, frequency pairing
 *	to get K most frequent, then we just iterate through this file, and push to a min heap.
 *	if min heap size gets bigger than K, then we peek at the minimum element, and if that element is smaller than
 *	the current element, pop the min from heap, and push the new item!
*/

public class Driver {
	public static void main( String[] args ) {
		if( args.length < 2 ) {
			System.out.println( "Need input in the form of <K> <file1>, <file2>, ..." );
			return;
		}
		List<String> fileList = new ArrayList<String>( Arrays.asList( args ) );
		// remove K input from fileList
		fileList.remove( 0 );
		
		// get folder list by traversing the list. Some of the items WILL be a directory, therefore, traverse its children to get only all files!
		Set<String> files = Driver.DFSFolderList( fileList );
		int K = Integer.parseInt( args[ 0 ] );
		try {
			if( K < 1 ) {
				System.out.println( "First parameter is required to be a positive integer!" );
				System.out.println( "Need input in the form of <K> <file1>, <file2>, ..." );
				return;
			}
		} catch( NumberFormatException e ) {
			System.out.println( "First parameter is required to be an integer!" );
			System.out.println( "Need input in the form of <K> <file1>, <file2>, ..." );
			return;
		}

		JobConf conf = new JobConf( Driver.class );
		try {
			// setting up prior to running hadoop job...
			FileSystem hdfs = FileSystem.get( conf );
			Driver.cleanHDFSDirectory( hdfs, files );
		} catch( Exception e ) {
			System.out.println( "File does not exist!" );
		}
		
		conf.setJobName( "wordcount" );
		
		conf.setOutputKeyClass( Text.class );
		conf.setOutputValueClass( IntWritable.class);
		
		FileInputFormat.addInputPath( conf, new Path( "input" ) );
		FileOutputFormat.setOutputPath( conf,  new Path( "output" ) );
		
		conf.setMapperClass( MapperFunction.class );
		
		conf.setReducerClass( ReducerFunction.class );
		conf.setCombinerClass( ReducerFunction.class );
		
		conf.setInputFormat( TextInputFormat.class );
		conf.setOutputFormat( TextOutputFormat.class );
		
		try {
			JobClient.runJob( conf );
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		try {
			FileSystem hdfs = FileSystem.get( conf );
			// get result from hdfs file system
			Driver.getMapReduceResult( hdfs );
			
			// use a minHeap and limit its size to K. If it gets bigger, peek the lowest occuring item and if that value 
			// is less than the current word, then kick that off, and replace with the new item!
			Driver.getKMostFrequent( K );
			
		} catch ( Exception e ) {
			System.out.println( e.getStackTrace() );
		}
		
	}

	// using set since we don't want to revisit!
	// possible that user passes in a folder which has a file, and then passes that same file!
	public static Set<String> DFSFolderList( List<String> fileList ) {
		if( fileList.size() == 0 ) {
			return ( Set )( new HashSet<String>() );
		}
		Set<String> files = new HashSet<String>();
		List<String> childrenFiles = new ArrayList<String>();
		
		int index = 0;
		while( fileList.size() > 0 ) {
			String fileName = fileList.remove( index );
			File file = new File( fileName );
			// if directory, explore its nesting!
			if( file.isDirectory() ) {
				for( File f : file.listFiles() ){
					childrenFiles.add( f.getAbsolutePath() );
				}
				// add all children files into result Set
				Set<String> nestingFileList = Driver.DFSFolderList( childrenFiles );
				files.addAll(nestingFileList);
			}
			// else file exists and it's just a regular! 
			// add to our result list 
			else if( file.exists() && file.isFile() ) {
				files.add( fileName );
			}
			// if non-existent, just skip!
		}
		return files;
	}
	public static void cleanHDFSDirectory( FileSystem hdfs, Set<String> filesToCopy ) {
		try {
			String prevResult = System.getProperty("user.dir") + "/part-00000";
			File prevRes = new File( prevResult );
			if( prevRes.exists() ) {
				prevRes.delete();
			}
			Path homeDir = hdfs.getHomeDirectory();
			
			String[] foldersToClean = { "output", "input" };
			for( int i = 0; i < foldersToClean.length; i ++ ) {
				Path directoryToDelete = new Path( foldersToClean[ i ] );
				if( hdfs.exists( directoryToDelete ) ) {
					hdfs.delete( directoryToDelete, true );
				}				
			}
			Path inputPath = new Path( "/input" );
			inputPath = Path.mergePaths( homeDir, inputPath );
			
			// recreate input folder!
			hdfs.mkdirs( inputPath );
			
			// get all local files and copy to hdfs file system!
			for( String fileName : filesToCopy ) {
				// copy inputs from local!
				Path localFile  = new Path( fileName );
				hdfs.copyFromLocalFile( localFile, inputPath );
			}	
		} catch ( Exception e ) {
			System.out.println( e.getStackTrace() );
		}
	}
	public static String getMapReduceResult( FileSystem hdfs ) throws IOException {
		try {
			String localWorkingDir = System.getProperty("user.dir");
			Path localFile = new Path( localWorkingDir );
			
			Path input = hdfs.getHomeDirectory();
			Path _input = new Path( "/output/part-00000" );
			input = Path.mergePaths( input, _input );
			
			hdfs.copyToLocalFile( input, localFile );
			return localWorkingDir + "/output/part-00000";
		} catch( Exception e ) {
			System.out.println( e.getStackTrace() );
		}
		return "";
	}
	public static void getKMostFrequent( int K ) throws FileNotFoundException, IOException {

		// use a PriorityQueue as a minHeap
		PriorityQueue< WordFrequencyPair > minHeap = new PriorityQueue< WordFrequencyPair >();
		
		String fileName = System.getProperty( "user.dir" ) + "/part-00000";
		File file = new File( fileName );
		try ( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
		    String line;
		    int heapSize = 0;
		    while ((line = br.readLine()) != null) {
		    	String[] pairing = line.split( "\\s+" );;  
		    	WordFrequencyPair keyValuePair = new WordFrequencyPair( pairing[ 0 ], Integer.parseInt( pairing[ 1 ] ) );
		    	WordFrequencyPair keyValuePair2;
		    	
		    	// if our heapSize goes over K, compare min with current word!
		    	if( heapSize == K ) {
		    		keyValuePair2 = minHeap.peek();
		    		
		    		// kick off old item from heap and replace with this current item!
		    		if( keyValuePair2.frequency < keyValuePair.frequency ) {
		    			minHeap.poll();
		    			minHeap.add( keyValuePair );
		    		}
		    	}
		    	else {
		    		minHeap.add( keyValuePair );
		    		heapSize += 1;
		    	}
		    }
		}
		// print out result!
		while( !minHeap.isEmpty() ) {
			WordFrequencyPair item = minHeap.poll();
			System.out.println( "word <" + item.word + "> occurred " + Integer.toString( item.frequency ) + " times" );
		}
	}
}