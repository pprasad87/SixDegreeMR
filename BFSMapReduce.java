package sixdegrees;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BFSMapReduce {

	public static class BFSMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Node node = new Node(value.toString());

			// check adjacent nodes
			if (node.getColor() == Colors.GRAY) {
				for (int v : node.getAdjacent()) {
					Node vnode = new Node(v);
					vnode.setDist(node.getDist() + 1);					
					vnode.setColor(Colors.GRAY);					
					context.write(new IntWritable(vnode.getId()), new Text(
							vnode.getLine()));					
				}
				node.setColor(Colors.BLACK);
			}

			// send input node as is			
			context.write(new IntWritable(node.getId()),
					new Text(node.getLine()));

		}
	}

	public static class BFSReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			List<Integer> edges = null;
			int distance = 9999; 
			
			Colors color = Colors.WHITE;
			
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				Text value = itr.next();
			
				Node u = new Node(key.get() + " " + value.toString());
				
				if (u.getAdjacent().size() > 0) {
					edges = u.getAdjacent();
				}

				// Save the minimum distance
				if (u.getDist() < distance) {
					distance = u.getDist();
			
				}

				// Save the darkest color
				if (u.getColor().ordinal() > color.ordinal()) {
					color = u.getColor();
				}
			}
			
			Node n = new Node(key.get());
			n.setDist(distance);
			n.setAdjacent(edges);
			n.setColor(color);
			Text text = new Text(n.getLine());
			context.write(key, text);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: adjacencylist <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(BFSMapReduce.class);
		job.setMapperClass(BFSMapper.class);
		// No Combiner
		job.setReducerClass(BFSReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
