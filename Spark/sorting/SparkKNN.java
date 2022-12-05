import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;


// class of the BFS-based shortest route
public class SparkKNN {
    // main function
    // args:
    // args[0] k
    public static void main(String[] args) throws FileNotFoundException{
        // Start Spark and read training points
        String inputFile = "train.csv";
        SparkConf conf = new SparkConf().setAppName("Spark K-NN");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile(inputFile);
        int k = Integer.parseInt(args[0]);
        // Points are stored as <"X Y","Class"> for groundTruth and result
        HashMap<String, String> groundTruth = new HashMap<>();
        HashMap<String, String> result = new HashMap<>();
        // targets contains the points to be classified
        ArrayList<Point> targets = new ArrayList<>();

        // Read test points
        Scanner input = new Scanner(new File("test.csv"));
        int numTest = input.nextInt();
        input.nextLine();
        for (int i = 0; i < numTest; i++) {
            String line = input.nextLine();
            String[] words = line.split(",");
            targets.add(new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1]),
                    Double.parseDouble(words[2])));
            groundTruth.put(words[0] + " " + words[1] + " " + words[2], words[3]);
        }

        
        final String classNames[] = { "clear", "clouds", "rain" };
        
        //PrintWriter output = new PrintWriter("prediction.txt");

        // start a timer
        long startTime = System.currentTimeMillis();

        // Process the input into JavaPairRDD, each pair is a Point-classNum pair.
        JavaPairRDD<Point,Integer> train = lines.mapToPair(line -> {
            String words[] = line.split(",");
            Double x = Double.parseDouble(words[0]);
            Double y = Double.parseDouble(words[1]);
            Double z = Double.parseDouble(words[2]);
            int classNum;
            if(words[3].equals("clear")){
                classNum=0;
            }
            else if(words[3].equals("clouds")){
                classNum=1;
            }
            else{
                classNum=2;
            }
            return new Tuple2<Point,Integer>(new Point(x,y,z),classNum);
        });

        train.cache();

        // Process each point
        for (Point target : targets) {
            // Calculate distance
            JavaPairRDD<Double,Integer> distances = train.mapToPair(pointPair-> {
                return new Tuple2<Double,Integer>(pointPair._1.dist(target),pointPair._2);
            });

            // Find k closest points by sorting
            List<Tuple2<Double,Integer>> firstKClosest = distances.sortByKey().take(k);
            
            // Do majority voting
            // Find max class number
            int maxClass = 0;
            for (int i = 0; i < k; i++) {
                if (firstKClosest.get(i)._2 > maxClass) {
                    maxClass = firstKClosest.get(i)._2;
                }
            }
            // Vote
            int votes[] = new int[maxClass+1];
            for (int i = 0; i < k; i++) {
                votes[firstKClosest.get(i)._2] += 1;
            }
            // Find the winning class
            int winClass=0;
            int maxVote=0;
            for (int i = 0; i < maxClass + 1; i++) {
                if (votes[i] > maxVote) {
                    maxVote = votes[i];
                    winClass = i;
                }
            }
            // if (target.x == 93.0 && target.y == 1014.0 && target.z == 289.29) {
            //     System.out.print("Vote box: ");
            //     for (int i = 0; i < maxClass+1; i++) {
            //         System.out.print(votes[i]);
            //     }
            //     System.out.println(" ");

            //     for (Tuple2<Double, Integer> elem : firstKClosest) {
            //         System.out.println("dist: "+elem._1+", class: "+elem._2);
            //     }
            // }

            result.put(target.x + " " + target.y + " " + target.z, classNames[winClass]);
            //output.println(target.x + "," + target.y + "," + target.z + "," + classNames[winClass]);
        }
        
        System.out.println("Elapsed Time: " + (System.currentTimeMillis() - startTime));
        //output.close();

        // Evaluate acc (accuracy)
        double acc=0;
        for (Entry<String,String> e : result.entrySet()) {
            if (groundTruth.get(e.getKey()).equals(e.getValue())) {
                acc++;
            }
        }
        acc /= numTest;
        System.out.println("acc: " + acc);

    }
}
