
/*
 * WordCountMapper.java
 *
 * Created on Oct 22, 2012, 5:31:10 PM
 */

package org.sample;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author mac
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountMapper");
	 private String hoppair = new String();
	public static  Map<String, Object> mymap =  new HashMap<String,Object>();
	 
	
	 
	 
    protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
    	
    	String[] AdjacencyList = value.toString().split(":");
    	String VertexID =  AdjacencyList[0];
    	System.out.println(VertexID);
    	
    	for (String keyword :  AdjacencyList[1].toString().split(",")) {
    		if (keyword.length() > 0) {
    		 hoppair = hoppair +"|" +keyword;
    			System.out.println(keyword);
    			
    			context.write(new Text(VertexID) ,new Text(keyword));
    		}
    	}
    	
    	mymap.put(VertexID, hoppair);
    	hoppair ="";
    }
}
