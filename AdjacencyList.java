package sixdegrees;
/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AdjacencyList {
	
	private static final int SOURCE = 1;

  public static class AdjacencyMapper 
       extends Mapper<Object, Text, IntWritable, IntWritable>{      
      
	  // Local aggregation for Map task?
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	CSVParser csv = new CSVParser();

		// each record from CSV file
		String[] str = csv.parseLine(value.toString());
		String startNode = str[0];
		String adjacentNode = str[1];
		// emit from map
		context.write(new IntWritable(Integer.parseInt(startNode)), 
				new IntWritable(Integer.parseInt(adjacentNode)));
		context.write(new IntWritable(Integer.parseInt(adjacentNode)), 
				new IntWritable(Integer.parseInt(startNode)));
    }
  }
  
  public static class AdjacencyReducer 
       extends Reducer<IntWritable,IntWritable,Text,Text> {
   
    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuffer buf = new StringBuffer();      
      buf.append(key.toString());
      buf.append(" ");
           
      Set<Integer> edgeSet = new HashSet<Integer>();
      for (IntWritable val : values) {
    	 edgeSet.add(val.get());
      }
      List<Integer> edgeList = new ArrayList<Integer>();
      edgeList.addAll(edgeSet);
      int index=0;
      while (index < edgeList.size())
      {
    	  if(index < edgeList.size()-1){
    		  buf.append(edgeList.get(index).toString()).append(",");
    	  }
    	  else
    		  buf.append(edgeList.get(index).toString());
    	  index++;
      }
      // distance
      buf.append(":");
      if (key.get() == SOURCE)
    	  buf.append("0");
      else
    	  buf.append("9999"); 
      // color      
      buf.append(":");
      if (key.get() == SOURCE)
    	  buf.append(Colors.GRAY); 
      else
    	  buf.append(Colors.WHITE);       
      context.write(null, new Text(buf.toString()));
    }
  }
  
  // custom partitioner 
  public static class CustomPartitioner extends Partitioner<IntWritable, IntWritable> {

	@Override
	// Divide into partitions based on character
	public int getPartition(IntWritable key, IntWritable value, int numReduceTasks) {		
		return (key.get()%numReduceTasks);
	}
	  
  }
/*
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: adjacencylist <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Adjacency List");
    job.setJarByClass(AdjacencyList.class);
    job.setMapperClass(AdjacencyMapper.class);   
    job.setPartitionerClass(CustomPartitioner.class);
    job.setReducerClass(AdjacencyReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  */
  public void execute(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	  Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: adjacencylist <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Adjacency List");
	    job.setJarByClass(AdjacencyList.class);
	    job.setMapperClass(AdjacencyMapper.class);	    
	    job.setPartitionerClass(CustomPartitioner.class);
	    job.setReducerClass(AdjacencyReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(6);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1] + "0"));
	    job.waitForCompletion(true);
  }
}
