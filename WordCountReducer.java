
/*
 * WordCountReducer.java
 *
 * Created on Oct 22, 2012, 5:33:40 PM
 */

package org.sample;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author mac
 */
public class WordCountReducer extends Reducer<Text,Text,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("org.sample.WordCountReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
    	boolean first = true;
    	 StringBuilder toReturn = new StringBuilder();
         Set<String> graphIds = new HashSet<String>();
         Iterator<String> graphIdItr;
         String value = new String();
         Set<String> h = new HashSet<String>();
         ArrayList<String> hop2list = new ArrayList<String>();
         for (Text val : values) {
        	 // System.out.println(val);//c2 c3 c5
        	   value = WordCountMapper.mymap.get(val.toString()).toString();
        	   StringTokenizer st = new StringTokenizer(value, "|");
        	   while(st.hasMoreTokens())
        		   hop2list.add(st.nextToken());
        	  
             graphIds.add(val.toString());
             
         }
         
         for(String i : graphIds){
        	 hop2list.removeAll(Collections.singleton(i.toString()));
        	// System.out.println("adjacent values :"+i);
         }
         
         
         
         hop2list.removeAll(Collections.singleton(key.toString()));
         String mytemp = "";
         Set<String> finaloutput = new HashSet<String>();
         
         
        
         
         for (String s : hop2list) {
        	 System.out.println("The 2 hop values of " + key + ":");
        	 finaloutput.add("["+s + " :" + Collections.frequency(hop2list, s)+"]");
        	 
        	}
         
         TreeSet<String> myTreeSet = new TreeSet(new MyComp());
         myTreeSet.addAll(finaloutput);
        
         
         for (String s : myTreeSet) 
        	 mytemp = mytemp + "," + s;
         
         
         mytemp = mytemp  + " =>";
         mytemp = mytemp.startsWith(",") ? mytemp.substring(1) : mytemp;
         
         
         context.write(new Text(mytemp), key);
         
         graphIdItr = graphIds.iterator();
         while (graphIdItr.hasNext()) {
             
            
             if (!first){
                 toReturn.append('^');
             }

             first = false;
            
             toReturn.append(graphIdItr.next());

         }
         String intersect = new String(toReturn);
       
    //	context.write(key, new Text(intersect));
    }
}

class MyComp implements Comparator<String>{

    @Override
    public int compare(String str1, String str2) {
    	
    	String[] temp = str1.split(":");
    	String[] temp1 = str2.split(":");
    	System.out.println(temp[1].substring(0,1)+"=>"+ str1);
    	System.out.println(temp1[1].substring(0,1)+"=>"+ str2);
    	
    	
    	if( Integer.parseInt(temp[1].substring(0,1)) == Integer.parseInt((temp1[1].substring(0,1))))
    		return  temp[0].compareTo(temp1[0]);
    		
        if( Integer.parseInt(temp[1].substring(0,1)) > Integer.parseInt((temp1[1].substring(0,1))))
        		return -1;
        else 
        	return 1;
    }

}
