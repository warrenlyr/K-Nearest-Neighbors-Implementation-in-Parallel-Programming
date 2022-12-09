/*
 * -------------------------------------------------------------------------------------------------
 * testMPI.java
 * Term: Autumn 2022
 * Class: CSS 534 – Parallel Programming In Grid And Cloud
 * HW: Program 5 - Final Project
 * Author: Warren Liu
 * Date: 12/07/2022
 * -------------------------------------------------------------------------------------------------
 * KNN Graph in parallel programming.
 * Part 4: Parallel in MASS.
*/
import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.lang.Math;
import java.sql.Timestamp;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Place;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;


public class KnnMASS{
    //
    private static final String NODE_FILE = "nodes.xml";

    public static class Node implements Serializable, Comparable<Node>{
        // Coordinates
        double x;
        double y;
        double z;
        // Class name
        String className;
        String newClassName;
        // This node's distance to target node
        double distance_to_target;
    
        public Node(){
            x = 0.0;
            y = 0.0;
            z = 0.0;
            className = "";
            distance_to_target = 0.0;
        }
    
        public Node(double x, double y, double z, String className){
            this.x = x;
            this.y = y;
            this.z = z;
            this.className = className;
            this.newClassName = "";
            this.distance_to_target = 0.0;
        }
    
        public Node(double x, double y, double z, String className, double distance_to_target){
            this.x = x;
            this.y = y;
            this.z = z;
            this.className = className;
            this.newClassName = "";
            this.distance_to_target = distance_to_target;
        }
    
    
        // Get current node's distance to target node
        public double getDistance(){
            return this.distance_to_target;
        }
    
    
        // Get current node class name
        public String getClassName(){
            return this.className;
        }
    
    
        // Helper function for debug
        public String getResultForValidation(){
            return (this.x + "," + this.y + "," + this.x + "," + this.newClassName);
        }
    
    
        // Set current node's new class name
        public void setNewClassName(String newClassName){
            this.newClassName = newClassName;
        }
    
    
        public Double distance(Node another){
            double x = this.x - another.x;
            double y = this.y - another.y;
            double z = this.z - another.z;
            return Math.sqrt(x*x + y*y + z*z);
        }
    
    
        // Helper function for printing node details
        @Override
        public String toString(){
            return ("Node [x=" + this.x + ", y=" + this.y + ", z=" + this.z + 
            ", class=" + this.className + ", newClass=" + this.newClassName + ", distance=" + this.distance_to_target + "]");
        }
    
    
        // Customized compare, for treeMap use
        @Override
        public int compareTo(Node node){
            return (int) (this.distance_to_target - node.getDistance());
        }
    }


    /*
     * Helper function to sort Node array by nodes' distance_to_target.
     */
    public static class SortByNodeDistance implements Comparator<Node> {
        public int compare(Node a, Node b){
            return a.distance_to_target < b.distance_to_target ? -1 : 1;
        }
    }


    /*
     * Class to get a target node's new class name from the give
     * top_k_neighbor array. The size of this array should be k.
     * All neighbors in this array do the majority vote.
     * The most common class among the neighbors becomes the new
     * class name for the target node.
     * If two classes' votes are the same, we go by the alphabet order.
     * Example:
     * In our case, our labels are [clear, clouds, rain].
     * So if clear gets 4 votes and clouds gets 4 votes,
     * the new class name will be clear.
     * 
     * @param Node node: the target node.
     * @param Node[] top_k_neighbor_arr: the target node's top k neighbors.
     * 
     * @return none
     */
    public static void getNodeNewClassByTopKNeighbors(Node node, Node[] top_k_neighbor_arr){

        HashMap<String, Integer> count = new HashMap<String, Integer>();

        for(int i = 0; i < top_k_neighbor_arr.length; i ++){
            String classname = top_k_neighbor_arr[i].getClassName();

            if(count.containsKey(classname)){
                count.put(classname, count.get(classname) + 1);
            }
            else{
                count.put(classname, 1);
            }
        }

        TreeMap<String, Integer> sortedCount = new TreeMap<String, Integer>();
        sortedCount.putAll(count);

        // Get max-vote. If votes are equal, use alphabet-order
        int max_vote = 0;
        String max_vote_class_name = "";
        for(java.util.Map.Entry<String, Integer> pair: sortedCount.entrySet()){
            if(pair.getValue() > max_vote){
                max_vote = pair.getValue();
                max_vote_class_name = pair.getKey();
            }
        }

        node.setNewClassName(max_vote_class_name);
    }


    /*
     * Class to evaluate the result generated by KNN.
     * For each node in the target(test) group, if its new class name
     * is equal to its original class name, we say the prediction
     * is correct. Otherwise, the prediction is wrong.
     * The correctness is calculated by: 
     * correct# / (correct# + wrong#)
     * 
     * @param Node[] nodeArr: A 1D node array, contains the all target(test) nodes.
     * 
     * @return double correctness.
     */
    public static double evaluateKnnCorrectness(Node[] nodeArr){
        double correct_count = 0.0;
        double wrong_count = 0.0;

        for(int i = 0; i < nodeArr.length; i ++){
            // System.out.println(nodeArr[i].newClassName + " " + nodeArr[i].className);

            if(nodeArr[i].newClassName.equals(nodeArr[i].className)){
                correct_count += 1.0;
            }
            else{
                wrong_count += 1.0;
            }
        }

        return (correct_count / (correct_count + wrong_count));
    }


    public static class TrainGroup extends Place{
        public static final int init_ = 0;
        public static final int computeDistance_ = 1;
        public static final int collectNode_ = 2;
        public static final int collectDistance_ = 3;

        private Node node;

        public TrainGroup(Object args) {}

        public Object callMethod(int funcId, Object args){
            switch(funcId){
                case init_: return init(args);
                case computeDistance_: return computeDistance(args);
                case collectNode_: return collectNode(args);
                case collectDistance_: return collectDistance(args);
            }
            return null;
        }

        public Object init(Object args){
            // System.out.println("here?1");
            this.node = (Node) args;
            // System.out.println("here?2");
            return null;
        }

        public Object computeDistance(Object args){
            this.node.distance_to_target = this.node.distance((Node) args);
            return null;
        }

        public Node collectNode(Object args){
            return this.node;
        }

        public Double collectDistance(Object args){
            return this.node.distance_to_target;
        }
    }


    public static void main(String[] args) throws IOException{
        if(args.length < 3){
            System.err.println("Usage: mpirun -n <node#> java knnJavaMPI_v2 <input_test_group_file> <input_train_group_file> <top_k>");
            System.exit(-1);
        }

        int k = 0;
        int train_size = 0;
        int test_size = 0;

        // index-0: test group
        // index-1: train group
        ArrayList<ArrayList<Node>> group = new ArrayList<ArrayList<Node>>();
        for(int i = 0; i < 2; i ++){
            group.add(new ArrayList<Node>());
        }


        String train_file_name = "";
        String test_file_name = "";

        test_file_name = args[0];
        train_file_name = args[1];
        k = Integer.parseInt(args[2]);

        try{
            File test_file = new File(test_file_name);
            Scanner sc_test = new Scanner(test_file);
            test_size = sc_test.nextInt();
            sc_test.nextLine();

            while(sc_test.hasNextLine()){
                String[] line = sc_test.nextLine().split(",", -1);
                Node node = new Node(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), line[3]);
                group.get(0).add(node);
            }

            File train_file = new File(train_file_name);
            Scanner sc_train = new Scanner(train_file);
            train_size = sc_train.nextInt();
            sc_train.nextLine();

            while(sc_train.hasNextLine()){
                String[] line = sc_train.nextLine().split(",", -1);
                Node node = new Node(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), line[3]);
                group.get(1).add(node);
            }
        }
        catch(FileNotFoundException e){
            System.err.println("Error open input file: " + e);
        }

        MASS.setNodeFilePath(NODE_FILE);
		MASS.setLoggingLevel(LogLevel.ERROR);
        MASS.init();

        long start_time_all = System.currentTimeMillis();

        ArrayList<ArrayList<Node>> res = new ArrayList<ArrayList<Node>>();
        for(int i = 0; i < test_size; i ++){
            res.add(new ArrayList<Node>());
        }

        Places knn_place = new Places(1, TrainGroup.class.getName(), null, train_size);
        Object[] train_group = group.get(1).toArray();
        knn_place.callAll(TrainGroup.init_, train_group);

        // For each target node
        for(int i = 0; i < test_size; i ++){
            // Calculate each train node (in the matrix) distance to this target node
            knn_place.callAll(TrainGroup.computeDistance_, group.get(0).get(i));
            // Collect calculated distance from the matrix
            Object[] temp = knn_place.callAll(TrainGroup.collectNode_, group.get(1).toArray());
            // for(Object x: temp){
            //     System.out.println((Double) x);
            // }
            ArrayList<Node> res_local = new ArrayList<Node>();
            for(Object x: temp){
                res_local.add((Node) x);
            }
            Collections.sort(res_local,  Comparator.comparing(Node::getDistance));
            for(int j = 0; j < k; j ++){
                res.get(i).add(res_local.get(j));
            }
        }

        // for(int i = 0; i < test_size; i ++){
        //     for(int j = 0; j < k; j ++){
        //         System.out.print(res.get(i).get(j));
        //     }
        //     System.out.println();
        // }

        for(int i = 0; i < test_size; i ++){
            getNodeNewClassByTopKNeighbors(group.get(0).get(i), res.get(i).toArray(new Node[res.get(i).size()]));
        }

        // Calculate the correctness
        double acc = evaluateKnnCorrectness(group.get(0).toArray(new Node[group.get(0).size()]));

        long end_time_all = System.currentTimeMillis();
        System.out.println("Accuracy: " + acc);
        System.out.println("Elapsed time (Total) = " + (end_time_all - start_time_all));

        MASS.finish();
    }
}