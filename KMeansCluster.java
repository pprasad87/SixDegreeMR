import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeansCluster extends Configured implements Tool {

	// Centers for K-Means clustering
	public static double centers[] = { 10000.0, 1000.0, 100.0 };
	public static double prevCenters[] = { 0.0, 0.0, 0.0 };

	// Mapper
	public static class KMeanMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// compute input record r’s distance to
			// each center, emit (c,r) where c is the closest center
			double currentValue = (double) Integer.parseInt((value.toString()
					.split(" "))[1]);

			double tempMin = 9999.0;
			int index = 0;
			for (int i = 0; i < centers.length; i++) {
				double dist = Math.abs(centers[i] - currentValue);
				if (dist < tempMin) {
					tempMin = dist;
					index = i;
				}
			}

			// emit
			context.write(new IntWritable(index), new Text(value.toString()));
		}

	}

	// Reducer
	public static class KMeanReducer extends
			Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			// re-compute center c as
			// the average of the records r i in cluster “c”

			int clusterID = key.get();

			StringBuffer sb = new StringBuffer();
			double sum = 0.0;
			int count = 0;
			for (Text t : values) {
				String str = t.toString();
				double d = (double) Integer.parseInt((str.split(" "))[1]);
				sum = sum + d;
				count++;
				sb.append((str.split(" "))[0]).append(" ");
			}
			double avg = sum / count;

			// update the centers
			prevCenters[clusterID] = centers[clusterID];
			centers[clusterID] = avg;
			context.write(new Text("new center " + avg),
					new Text("list " + sb.toString()));
		}
	}

	public int run(String[] args) throws Exception {

		// get the friends data
		Friends f = new Friends();
		f.execute(args);

		int iteration = 0;
		
		System.out.println("K means iterations");
		while (iteration ==0  || !converges())  {

			KMeansCluster kmeans = new KMeansCluster();
			boolean status = kmeans.execute(args, iteration);
			if (status) {
				System.out.println(centers[0] + " " + centers[1] + " "
						+ centers[2]);
				System.out.println(iteration + "iteration done");
				iteration++;				
			} else {
				System.out.println("Error in Driver Program");
				System.exit(0);
			}
		}

		return 0;
	}

	// Check the convergence condition
	public static boolean converges() {
		for (int i = 0; i < centers.length; i++) {
			// range < 10
			if (centers[i] > 10000.0) {
				if (((centers[i] % 100) != (prevCenters[i] % 100)) )
					return false;
			}
			// range < 10
			else if (centers[i] > 1000.0) {
				if (((centers[i] % 10) != (prevCenters[i] % 10)))
					return false;
			}
			// range < 10
			else 
			{	if ((((centers[i]*10) % 10) != ((prevCenters[i]*10) % 10)))
					return false;
			}
		}
		return true;
	}

	public boolean execute(String args[], int itr) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: kmeans <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "KMeans Clustering");

		job.setJarByClass(KMeansCluster.class);
		job.setMapperClass(KMeanMapper.class);
		job.setReducerClass(KMeanReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + itr));
		boolean status = job.waitForCompletion(true);
		return status;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: KMeansDriverProgram <in> <out>");
			System.exit(2);
		}

		int res = ToolRunner
				.run(new Configuration(), new KMeansCluster(), args);
		System.exit(res);
	}
}
