// FindAndExcludeKeys
// Please change the file name to "WordCount.java" for it to run properly. 
// The record that I process looks something like：0CBA75350DB6183D88C4464A64A4807E|D556BF44DE7DE5060D11CD1B9228E8EC|20161003101641|20161003101642|20161003101641|MobileMap|http://client.map.baidu.com/su?st=0&highlight_flag=2&rp_format=pb&qt=sug&l=17&cid=131&loc=%2812961586.237293%2C4835724.332161%29&type=0&b=%2812962203%2C4827250%3B12963148%2C4828927%29&wd=a&mb=iPhone5%2C2&os=iphone8.200000&sv=9.5.0&net=11&resid=01&cuid=b111f8ddacc8e570e59aab10b25147a2&bduid=5y-4sVQ5SC2aS&channel=1008648b&oem=&screen=%28640%2C1136%29&dpi=%28326%2C326%29&ver=1&sinan=%2FB8qu-HpjNdAvN%3DR3k6uRjmOS&ctm=1475461001.846000&sign=5c60c3e37ceed211818d48a2b2b5ae16|text/javascript|0||200·0
// I extracted the second item separated by "|" as the client ID, which in this case is "D556BF44DE7DE5060D11CD1B9228E8EC".

package org.jediael.hadoopdemo.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
	// Define the keywords to be included as well as the keywords to be excluded. 
	// Separate them using "\n". 
	public class ConstantClassField {  
    	public static final String FindKeyWords = "qunar\nelong\ntuniu\nly\n";
    	public static final String ExceptKeyWords = "ctrip\n";
	}  	
	
	//Mapper Function: get client ID
	// If client record contains items from FindKeyWords, assign value 1.
	// If client record contains items from ExceptKeyWords, assign value 0.
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
 
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text();
    
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // get each record
      while (itr.hasMoreTokens()) {
    	boolean findFlag = false;  // boolean for FindKeyWords
    	boolean exceptFlag = false; // boolean for ExceptKeyWords
    	String itrCopy = (String) itr.nextToken();
    	StringTokenizer findValue = new StringTokenizer(ConstantClassField.FindKeyWords);    
    	while (findValue.hasMoreTokens()){ // loop over each of the FindKeyWords
    		if (itrCopy.toLowerCase().contains(findValue.nextToken())) { //whether or not client record contains this keyword
    			findFlag = true;
    		}
		}
    	StringTokenizer exceptValue = new StringTokenizer(ConstantClassField.ExceptKeyWords, "\n");   
    	while (exceptValue.hasMoreTokens()){ //loop over each of the ExceptKeyWords 
    		if (itrCopy.toLowerCase().contains(exceptValue.nextToken())){ //whether or not client record contains this keyword
    			exceptFlag = true;
    		}
    	}
    	if (findFlag){ // if client record contains the FindKeyWords
    		StringTokenizer item = new StringTokenizer(itrCopy, "\\|");
    		item.nextToken();
        	String itemCopy = (String) item.nextToken() ; // extract the second item -- MDN ID
        	if (isMDN(itemCopy)) { // check if it has the same format of a standard MDN ID
        		word.set(itemCopy);
        		context.write(word, one); // key = MDN ID; Value = 1
        	}
    	}
    	if (exceptFlag){ // if client record contains the ExceptKeyWords
    		StringTokenizer item = new StringTokenizer(itrCopy, "\\|");
    		item.nextToken();
        	String itemCopy = (String) item.nextToken() ; // extract the second item -- MDN ID
        	if (isMDN(itemCopy)) { // check if it has the same format of a standard MDN ID
        		word.set(itemCopy);
        		context.write(word, zero); // key = MDN ID; Value = 0
        	}
    	}
    	}
    			
    	}
    	}
 
  
  public static boolean isMDN(String str) { // An MDN ID looks like "C493F4897A82B371C8E22808C356632A"
	    return (str.matches("[a-zA-Z0-9]+") && str.length() == 32);
	}
  

	//Reducer Function: exclude some clients that have ExceptKeyWords; sum the counts of the records for the rest.
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    // loop over each of the keys
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException{
      boolean excludeFlag = false; // boolean for whether the client has ever had a record containing ExceptKeyWords
      int sum = 0; // sum of the counts of records for the rest
      for (IntWritable val : values) { // loop over each of the values
    	if (val.get() == 0){
    		excludeFlag = true; 
    		break;
    	}
    	else {
            sum += val.get();
    	}
      }
      if (sum > 0 && !excludeFlag){ // write only those that has no records in the past containing ExceptKeyWords; print the count of visits.
    	  result.set(sum);
    	  context.write(key, result);
      }
    }
  }
 
 // Driver
  public static void main(String[] args) throws Exception {
	  // Get the configuration info
    Configuration conf = new Configuration();
    // Create a job
    Job job = Job.getInstance(conf, "find and exclude keywords");
    // Configure the classes for this job
    job.setJarByClass(WordCount.class);
    // Configure the mapper and reducer for this job
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    // Set up output class - key and value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // Set up input parameters - the location of the input file and the output file
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // Submit the job, wait for results, print it out and then end the program. 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
