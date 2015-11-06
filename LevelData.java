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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LevelData {
	
	// Average Degree of Separation
	
	// Global Counter
		public static enum NODE_COUNTER {
			TOTAL_NODES,
			DEGREE_COUNT
		};

	public static class LevelMapper extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		// Local aggregation for Map task?

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node node = new Node(value.toString());
			int level = node.getDist();
			// emit from map - level, id
			context.write(new IntWritable(level), new IntWritable(node.getId()));
			context.getCounter(NODE_COUNTER.TOTAL_NODES).increment(1);
			context.getCounter(NODE_COUNTER.DEGREE_COUNT).increment(level);
		}
	}

	public static class LevelReducer extends
			Reducer<IntWritable, IntWritable, Text, Text> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			StringBuffer buf = new StringBuffer();
			for (IntWritable val : values) {
				buf.append(val.get() + " "); 
			}
			context.write(new Text("Level: "+key.get()), new Text(buf.toString()));
		}
	}
	/*
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Level Data <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(LevelData.class);
		job.setMapperClass(LevelMapper.class);
		// No Combiner
		
		job.setReducerClass(LevelReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter c1 = (Counter) counters.findCounter(NODE_COUNTER.TOTAL_NODES);
		int totalNodes = (int) c1.getValue();
		System.out.println("Total nodes: "+totalNodes);
	}
	*/
	public void execute(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Level Data <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "level data");
		job.setJarByClass(LevelData.class);
		job.setMapperClass(LevelMapper.class);
		// No Combiner
		
		job.setReducerClass(LevelReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(6);
		FileInputFormat.addInputPath(job, new Path(args[1]+"6"));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "final"));		
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter c1 = (Counter) counters.findCounter(NODE_COUNTER.TOTAL_NODES);
		int totalNodes = (int) c1.getValue();
		Counter c2 = (Counter) counters.findCounter(NODE_COUNTER.DEGREE_COUNT);
		int totalDegrees = (int) c2.getValue();
		System.out.println("Average: "+(float)totalDegrees/(totalNodes-1));
	}
}
