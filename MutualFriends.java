import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text user = new Text();
		private Text flist = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				String fA = line[0];
				List<String> friends = Arrays.asList(line[1].split(","));
				for (String fB : friends) {
					if (Integer.parseInt(line[0]) < Integer.parseInt(fB))
						user.set(fA + "," + fB);
					else
						user.set(fB + "," + fA);
					flist.set(line[1]);
					context.write(user, flist);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text res = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			StringBuilder sb = new StringBuilder();
			for (Text f : values) {
				List<String> flist = Arrays.asList(f.toString().split(","));
				for (String friend : flist) {
					if (map.containsKey(friend))
						sb.append(friend + ',');
					else
						map.put(friend, 1);
				}
			}	

			res.set(new Text(sb.toString()));
			context.write(key, res);
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Mutual Friends <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}