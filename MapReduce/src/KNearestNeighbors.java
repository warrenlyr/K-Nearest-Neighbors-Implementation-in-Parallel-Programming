import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class KNearestNeighbors {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
		JobConf conf;
		private DoubleWritable distance = new DoubleWritable();
		private Text posting = new Text(); // testNodeNumber:trainNodeData

		public void configure(JobConf job) {
			this.conf = job;
		}

		public void map(LongWritable docId, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			// retrieve #testNode from JobConf
			int testNodeNum = Integer.parseInt(conf.get("testNodeNum"));
			// calculate the distance from every test node to the train node in current line and pass to reduce
			String line = value.toString();
			if (line != null) {
				String[] traindata = line.split(",");
				if (traindata != null && traindata.length >= 3) {
					double x = Double.valueOf(traindata[0]);
					double y = Double.valueOf(traindata[1]);
					double z = Double.valueOf(traindata[2]);
					for (int i = 0; i < testNodeNum; i++) {
						String testNode = conf.get("testNode" + i);
						String[] testdata = testNode.split(",");
						double dist = Math.pow(x - Double.valueOf(testdata[0]), 2) + Math.pow(y - Double.valueOf(testdata[1]), 2) + Math.pow(z - Double.valueOf(testdata[2]), 2);
						distance.set(dist);
						String post = i + ":" + line;
						posting.set(post);
						output.collect(distance, posting);
					}
				}
				
			}
		}
    }
	 
    public static class Reduce extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		JobConf conf;
		private List<List<String>> lists;
		private Text posting = new Text();
		private DoubleWritable testNodename = new DoubleWritable();

		public void configure(JobConf job) {
			this.conf = job;
			int testNodeNum = Integer.parseInt(conf.get("testNodeNum"));
			lists = new ArrayList<List<String>>();
			for (int i = 0; i < testNodeNum; i++) {
				List<String> list = new LinkedList<String>();
				lists.add(list);
			}
		}

		public void reduce(DoubleWritable distance, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			// the distances are already sorted, add them to the common list if the list size < k
			int k = Integer.valueOf(conf.get("k"));
			while (values.hasNext()) {
				String value = values.next().toString();
				if (value != null) {
					String[] arr = value.split(":"); // {testNodeNumber, trainNodeData}
					if (arr != null && arr.length > 1) {
						int testNode = Integer.valueOf(arr[0]);
						if (lists.get(testNode).size() == k) {
							// already have nearest k neighbors, just continue
							continue;
						}
						// this is a neighbor, add to the list, and check if size reach k then output
						String distanceAndNeighbor = "distance: " + distance.toString() + "|trainNode: " + arr[1];
						lists.get(testNode).add(distanceAndNeighbor);
						if (lists.get(testNode).size() == k) {
							String outputString = "";
							// use this hashmap to do majority vote
							HashMap<String, Integer> table = new HashMap<String, Integer>();
							for (String neighbor : lists.get(testNode)) {
								outputString += (neighbor + " ");
								String[] arr2 = neighbor.split(",");
								String className = arr2[arr2.length - 1];
								if (table.containsKey(className)) {
									table.put(className, table.get(className) + 1);
								} else {
									table.put(className, 1);
								}
							}
							String resultClass = "";
							Integer max = Integer.MIN_VALUE;
							for (HashMap.Entry<String, Integer> entry : table.entrySet()) {
								String className = entry.getKey();
								Integer count = entry.getValue();
								if (count > max) {
									resultClass = className;
									max = count;
								}
							}
							testNodename.set((double)testNode);
							String post = "result: " + resultClass + " k nearest neighbors: " + outputString;
							posting.set(post);
							output.collect(testNodename, posting);
						}
					}
				}
			}
		}
    }
    
    public static void main(String[] args) throws Exception {
		// input format:
		//  hadoop jar knearestneighbors.jar KNearestNeighbors input output k testNodePath
		JobConf conf = new JobConf(KNearestNeighbors.class);
		conf.setJobName("knearestneighbors");
		
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.set("k", args[2]); // k
		String testPath = args[3]; // testNodePath

		Date startTime = new Date();

		// reading the test node file and then pass into mapreduce using conf.set
		try {
			BufferedReader br = new BufferedReader(new FileReader(testPath));
			Integer num = Integer.valueOf(br.readLine());
			conf.set("testNodeNum", num.toString());
			for (int i = 0; i < num; i++) {
				String testNode = br.readLine();
				conf.set("testNode" + i, testNode);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		// run the job
		JobClient.runJob(conf);

		Date endTime = new Date();
		System.out.println( "Elapsed time = " + ( endTime.getTime( ) - startTime.getTime( ) ) );
    }
}


