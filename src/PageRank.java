import java.io.IOException;
import java.util.Collections;
import java.util.regex.Pattern;
import java.io.*;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.List;
import java.util.Comparator;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] nodes = value.toString().split(" \"\\[\'");
			String nodeId = nodes[0];
			String[] intermediary = nodes[1].split("\'\\]");
			String[] outLinks = intermediary[0].split("\', \'");

			double initRank = 1.0;
			context.write(new Text(nodeId), new Text("Ratio," + initRank));
											
			double ratio = initRank / outLinks.length;			
			for (int i = 0; i < outLinks.length; i++) {
				context.write(new Text(outLinks[i]), new Text("Ratio," + ratio));
			}

			context.write(new Text(nodeId), new Text("Out," + outLinks.length));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, String> {

		private Map<Text, String> countMap = new HashMap<Text, String>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double d = 0.85;
			int outLinks = 0;
			int inLinks = -1;
			double pagerank = 0.0;

			for (Text value : values)
			{
				String[] parts = value.toString().split(",");
				double _value = Double.parseDouble(parts[1]);
				
				if (value.toString().contains("Out"))
				{
					outLinks += _value;
				}
				else
				{
					inLinks++;
					pagerank += _value;
				}
			}
			
			pagerank = (1-d) + (d*pagerank);
			
			String value = pagerank + ", " + outLinks + ", " + inLinks;
			//context.write(key, value);
			countMap.put(new Text(key), new String(value));
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			List<Entry<Text, String>> countList = new ArrayList<Entry<Text, String>>(countMap.entrySet());
			Collections.sort(countList, new Comparator<Entry<Text, String>>(){
				public int compare( Entry<Text, String> o1, Entry<Text, String> o2) {
					String[] o1_parts = o1.getValue().split(",");
					double o1_rank = Double.parseDouble(o1_parts[0]);
					String[] o2_parts = o2.getValue().split(",");
					double o2_rank = Double.parseDouble(o2_parts[0]);
					
					//throw new Exception("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" + o1_rank + " ? " + o2_rank + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
					
					return ((o2_rank > o1_rank) ? 1 : 0);
				}
			});
			
			int length = countList.size();
			for (int i = 0; i < length && i < 500; i++)
			{
				Entry<Text, String> entry = countList.get(i);
				context.write(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
