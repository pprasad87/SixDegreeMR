import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sixdegrees.AdjacencyList;
import sixdegrees.BFSMapReduce.BFSMapper;
import sixdegrees.BFSMapReduce.BFSReducer;
import sixdegrees.LevelData;

/**
 * This is an example Hadoop Map/Reduce application.
 * 
 * It inputs a map in adjacency list format, and performs a breadth-first
 * search. The input format is ID EDGES|DISTANCE|COLOR where ID = the unique
 * identifier for a node (assumed to be an int here) EDGES = the list of edges
 * emanating from the node (e.g. 3,8,9,12) DISTANCE = the to be determined
 * distance of the node from the source COLOR = a simple status tracking field
 * to keep track of when we're finished with a node It assumes that the source
 * node (the node from which to start the search) has been marked with distance
 * 0 and color GRAY in the original input. All other nodes will have input
 * distance Integer.MAX_VALUE and color WHITE.
 */
public class SixDegreesDriver extends Configured implements Tool {

	public static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.examples.GraphSearch");

	static int printUsage() {
		System.out
				.println("graphsearch [-m <num mappers>] [-r <num reducers>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	public int run(String[] args) throws Exception {

		// pre processing:		
		AdjacencyList adj = new AdjacencyList();
		adj.execute(args);

		// start 6-level MR jobs
		int iteration = 1;

		System.out.println("iteration start");
		while (iteration <= 6) {

			// Configuration processed by ToolRunner
			Configuration conf = getConf();

			// Create a JobConf using the processed conf
			Job job = new Job(conf, "Driver");

			// Process custom command-line options
			Path in = new Path(args[1] 
					+ (iteration - 1));
			Path out = new Path(args[1] 
					+ (iteration));

			// Specify various job-specific parameters
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);
			job.setJarByClass(SixDegreesDriver.class);
			job.setMapperClass(BFSMapper.class);
			job.setReducerClass(BFSReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(6);

			// Submit the job, then poll for progress until the job is complete
			boolean status = job.waitForCompletion(true);
			if (status)
				iteration++;
			else {
				System.out.println("Error in Driver Program");
				System.exit(0);
			}
		}

		// post processing:	
		LevelData level = new LevelData();
		level.execute(args);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: DriverProgram <in> <out>");
			System.exit(2);
		}
		// args[0] -> actual input data file
		// args[1] -> folder path for subsequent outputs
		int res = ToolRunner.run(new Configuration(), new SixDegreesDriver(),
				args);
		System.exit(res);
	}

}