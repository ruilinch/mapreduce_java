// CategoryFrequencyCount
// Please change the file name to "WordCount.java" for it to run properly. 
// Count the Frequency for A Number of KeyWords
// The record that I process looks something like£º0CBA75350DB6183D88C4464A64A4807E|D556BF44DE7DE5060D11CD1B9228E8EC|20161003101641|20161003101642|20161003101641|MobileMap|http://client.map.baidu.com/su?st=0&highlight_flag=2&rp_format=pb&qt=sug&l=17&cid=131&loc=%2812961586.237293%2C4835724.332161%29&type=0&b=%2812962203%2C4827250%3B12963148%2C4828927%29&wd=a&mb=iPhone5%2C2&os=iphone8.200000&sv=9.5.0&net=11&resid=01&cuid=b111f8ddacc8e570e59aab10b25147a2&bduid=5y-4sVQ5SC2aS&channel=1008648b&oem=&screen=%28640%2C1136%29&dpi=%28326%2C326%29&ver=1&sinan=%2FB8qu-HpjNdAvN%3DR3k6uRjmOS&ctm=1475461001.846000&sign=5c60c3e37ceed211818d48a2b2b5ae16|text/javascript|0||200¡¤0
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
	
	public class ConstantClassField {  
    	public static final String FindKeyWords = "qunar\nelong\ntuniu\nly\nctrip\n";
	}  	
 
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
 
    private Text name = new Text();
    private final static IntWritable one = new IntWritable(1);
    
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	String itrCopy = (String) itr.nextToken();
    	StringTokenizer findValue = new StringTokenizer(ConstantClassField.FindKeyWords, "\n");    
    	while (findValue.hasMoreTokens()){
    		String findCopy = (String) findValue.nextToken();  
    		if (itrCopy.toLowerCase().contains(findCopy)) {
        		StringTokenizer item = new StringTokenizer(itrCopy, "\\|");
        		item.nextToken();
            	String itemCopy = (String) item.nextToken() ;
            	if (isMDN(itemCopy)) {
            		String str = itemCopy + " \t " + findCopy;
            		name.set(str);
            		context.write(name, one);
    		}
		}   	

    	}
    			
    	}
    	}
  }
  
  public static boolean isMDN(String str) {
	    return (str.matches("[a-zA-Z0-9]+") && str.length() == 32);
	}
  

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
 
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException{
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
 
 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}