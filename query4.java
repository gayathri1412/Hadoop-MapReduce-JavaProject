package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class query4 {
	
      public static class Transaction extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String [] line = value.toString().split(",");
		try{
		
		int a = Integer.parseInt(line[1]);
		output.collect(new Text(line[1]),new Text(one + "," + line[2]));
		}
		catch(NumberFormatException  e){
		
			output.collect(new Text(line[0]),new Text(line[4]+","+line[3]));
		}
      }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
      public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int num = 1;
		float i = 0;
		float min = 1000;
		float max = 0;
		
		String str = new String();		
		while (values.hasNext()) {
			String [] Trans = values.next().toString().split(",");
			i = Float.parseFloat(Trans[0]);
			if( i == 1.0 ) {
				
				if(max < Float.parseFloat(Trans[1]))
				{
					max = Float.parseFloat(Trans[1]);
				}
				if(min > Float.parseFloat(Trans[1]))
				{
					min = Float.parseFloat(Trans[1]);
				}
				
			}
			else {
				str = Trans[1];
				
			}
        }
        output.collect(new Text(str) , new Text(num+" , "+max+" , "+min));
      }
    }
	

    public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(query4.class);
	  
      conf.setJobName("query4");

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      //conf.setMapperClass(Customer.class);
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


