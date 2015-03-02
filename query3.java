package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class query3 {
	
	public static class Transaction extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String [] line = value.toString().split(",");
		try{
		int a = Integer.parseInt(line[1]);
		output.collect(new Text(line[1]),new Text(one + "," + line[2] + "," + line[3]));
		}
		catch(NumberFormatException  e){
			output.collect(new Text(line[0]),new Text(line[4]+","+line[1]));
		}
      }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int num = 0;
		float i = 0;
		float tot_sum=0;
                String minItem = "10";
		String str = new String();		
		while (values.hasNext()) {
			String [] Trans = values.next().toString().split(",");
			i = Float.parseFloat(Trans[0]);
			if( i == 1.0 ) {
				num += Integer.parseInt(Trans[0]);
				tot_sum += Float.parseFloat(Trans[1]);
                        if(Integer.parseInt(minItem) > Integer.parseInt(Trans[2]))                   {
                                 minItem = Trans[2];
                        }
			}
			else { 
                                
				str = Trans[1]+","+Trans[0];
			}
		
        } 
   output.collect(key, new Text(str+" , "+num + " , "+ tot_sum + "," + minItem));
      }
    }

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(query3.class);
	  
      conf.setJobName("query3");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Transaction.class);
      conf.setReducerClass(Reduce.class);
	  

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	  
      MultiFileInputFormat.addInputPath(conf, new Path(args[0]));
      MultiFileInputFormat.addInputPath(conf, new Path(args[1]));
      FileOutputFormat.setOutputPath(conf, new Path(args[2]));
      JobClient.runJob(conf);
    }
}


