/*
 * -------------------------------------------------------------------------------------------------
 * KnnMASS.java
 * Term: Autumn 2022
 * Class: CSS 534 – Parallel Programming In Grid And Cloud
 * HW: Program 5 - Final Project
 * Author: Warren Liu
 * Date: 12/07/2022
 * -------------------------------------------------------------------------------------------------
 * KNN Graph in parallel programming.
 * Part 4: Parallel in MASS.
 * Version v1: Places computation only.
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
    private static final String OUTPUT_PATH = "./output/";
    private static int WRITE_RESULT_TO_FILE = 0;
    private static int WRITE_RESULT_DETAILS_TO_FILE = 0;


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
            return (this.x + "," + this.y + "," + this.z + "," + this.newClassName);
        }
    
        // Set current node's new class name
        public void setNewClassName(String newClassName){
            this.newClassName = newClassName;
        }
    
        // Helper function for printing node details
        @Override
        public String toString(){
            return ("Node [x=" + this.x + ", y=" + this.y + ", z=" + this.z + 
            ", class=" + this.className + ", newClass=" + this.newClassName + ", distance=" + this.distance_to_target + "]");
        }

        public String print(){
            return ("Node [x=" + this.x + ", y=" + this.y + ", z=" + this.z + 
            ", class=" + this.className + ", distance=" + this.distance_to_target + "]");
        }
    
        // Customized compare, for treeMap use
        @Override
        public int compareTo(Node node){
            return (int) (this.distance_to_target - node.getDistance());
        }
    }


    /* Function to calculate the Euclidean Distance between two nodes.
     * 
     * @param Node a: the first node.
     * @param Node b: the second node.
     * 
     * @Return double distance: the Euclidean Distance between two nodes.
     */
    public static double distance(Node a, Node b){
        double x = a.x - b.x;
        double y = a.y - b.y;
        double z = a.z - b.z;
        return Math.sqrt(x*x + y*y + z*z);
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

        // To ensure the alphabet-order if votes are equal on the same class
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


    /*
     * Helper function to write knn result to file.
     * The result including the target nodes details after knn.
     * 
     * @param: Node[] node_arr: the array contains all target nodes.
     * @param int size: the size of rank.
     * @param int k: the k for knn.
     * 
     * @return bool: true if successfully write, false otherwise.
     */
    static public boolean writeKnnResultToFile(Node[] node_arr, int k){
        try{
            // Create output path if not exist
            File output_path = new File(OUTPUT_PATH);
            output_path.mkdir();

            // Create otuput file if not exist, over if exist
            String file_output_path = OUTPUT_PATH +"Result_k_" + k + ".txt";
            File output_file = new File(file_output_path);
            if(! output_file.exists()){
                output_file.createNewFile();
            }

            FileWriter fw = new FileWriter(output_file.getPath());
            BufferedWriter bw = new BufferedWriter(fw);

            for(Node target: node_arr){
                bw.write(target.getResultForValidation());
                bw.write("\n");
            }

            bw.close();
            return true;
        }
        catch(IOException e){
            return false;
        }
    }

    /*
     * Helper function to write result details to file.
     * The details including the target node, and it's top K neighbor nodes details.
     * 
     * @param ArrayList<Node> target_group: the array containing all target nodes.
     * @param ArrayList<ArrayList<Node>> neighbor_group: the 2D array containing all target nodes'
     *  top k neighbor nodes.
     * @param int size: the size of target_group (the number of target nodes).
     * @param int k: k of knn.
     * 
     * @return bool: true if successfully write, false otherwise.
     */
    static public boolean writeKnnResultDetailsToFile(ArrayList<Node> target_group, ArrayList<ArrayList<Node>> neighbor_group, int size, int k){
        try{
            // Create output path if not exist
            File output_path = new File(OUTPUT_PATH);
            output_path.mkdir();

            // Create otuput file if not exist, over if exist
            String file_output_path = OUTPUT_PATH +"Result_details_k_" + k + ".txt";
            File output_file = new File(file_output_path);
            if(! output_file.exists()){
                output_file.createNewFile();
            }

            FileWriter fw = new FileWriter(output_file.getPath());
            BufferedWriter bw = new BufferedWriter(fw);

            for(int i = 0; i < size; i ++){
                bw.write(target_group.get(i).getResultForValidation());
                bw.write(" --- \n");
                for(int j = 0; j < k; j ++){
                    bw.write(neighbor_group.get(i).get(j).print());
                    bw.write("\n");
                }
                bw.write("\n");
            }

            bw.close();
            return true;
        }
        catch(IOException e){
            return false;
        }
    }


    /*
     * Class to override Place class of MASS.
     */
    public static class TrainGroup extends Place{
        // Customized callAll() function ID
        public static final int init_ = 0;
        public static final int computeDistance_ = 1;
        public static final int collectNode_ = 2;
        public static final int collectDistance_ = 3;

        // The node to store in each element in the Places matrix
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

        // Init function, to assign a train node in train group to an index of Places matrix
        public Object init(Object args){
            this.node = (Node) args;
            return null;
        }

        // Function to update the distance of the train node store in this index to the given target node
        public Object computeDistance(Object args){
            this.node.distance_to_target = distance(this.node, (Node) args);
            return null;
        }

        // Function to collect all Nodes in the Places matrix
        public Node collectNode(Object args){
            return new Node(this.node.x, this.node.y, this.node.z, this.node.className, this.node.distance_to_target);
        }

        // Function to collect all Nodes' distance attribute in the Places matrix (not used)
        public Double collectDistance(Object args){
            return this.node.distance_to_target;
        }
    }


    public static void main(String[] args) throws IOException{
        // Validate args
        if(args.length < 3){
            System.err.println("Usage: mpirun -n <node#> java knnJavaMPI_v2 <input_test_group_file> <input_train_group_file> <top_k>" +
                "<optional: write_final_result_to_file> <optional: write_result_details_to_file>");
            System.err.println("top_K: int, the k of knn\n" +
                "write_final_result_to_file: int, 1 or 0 (default)\n" +
                "write_result_details_to_file: int, 1 or 0 (default)");
            System.exit(-1);
        }

        /* Init */
        String train_file_name = "";
        String test_file_name = "";

        int k = 0;
        int train_size = 0;
        int test_size = 0;

        test_file_name = args[0];
        train_file_name = args[1];
        k = Integer.parseInt(args[2]);

        if(args.length > 3){
            WRITE_RESULT_TO_FILE = Integer.parseInt(args[3]);
        }
        if(args.length > 4){
            WRITE_RESULT_DETAILS_TO_FILE = Integer.parseInt(args[4]);
        }

        // Init array to store all test nodes and train nodes
        // index-0: test group
        // index-1: train group
        ArrayList<ArrayList<Node>> group = new ArrayList<ArrayList<Node>>();
        for(int i = 0; i < 2; i ++){
            group.add(new ArrayList<Node>());
        }
        

        // Read input files
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


        // Start MASS
        MASS.setNodeFilePath(NODE_FILE);
		MASS.setLoggingLevel(LogLevel.ERROR);
        MASS.init();
        // Timer
        long start_time_all = System.currentTimeMillis();

        // Init a 2D array to store the result of knn
        ArrayList<ArrayList<Node>> res = new ArrayList<ArrayList<Node>>();
        for(int i = 0; i < test_size; i ++){
            res.add(new ArrayList<Node>());
        }

        // Init Places of size of train_group size
        Places knn_place = new Places(1, TrainGroup.class.getName(), null, train_size);
        // Pass the whole train_group to Places, so each index of matrix corresponding to a 
        // train node in the train_group
        Object[] train_group = group.get(1).toArray();
        knn_place.callAll(TrainGroup.init_, train_group);

        // Timer to collect time used while computing distance over the matrix
        long compute_distance_time = 0;
        // Timer to collect time used while collect nodes over the matrix
        long collect_node_time = 0;

        // For each target node
        for(int i = 0; i < test_size; i ++){
            // Calculate each train node (in the matrix) distance to this target node
            long start_time_compute_dis = System.currentTimeMillis();
            knn_place.callAll(TrainGroup.computeDistance_, group.get(0).get(i));
            long end_time_compute_dis = System.currentTimeMillis();

            // Collect calculated distance from the matrix
            long start_time_collect_node = System.currentTimeMillis();
            Object[] temp = knn_place.callAll(TrainGroup.collectNode_, group.get(1).toArray());
            long end_time_collect_node = System.currentTimeMillis();

            // Store the retured node array in the temp array, cast the Object element to Node element,
            ArrayList<Node> res_local = new ArrayList<Node>();
            for(Object x: temp){
                res_local.add((Node) x);
            }

            // Sort the temp array
            Collections.sort(res_local,  Comparator.comparing(Node::getDistance));

            // Get the top K node
            for(int j = 0; j < k; j ++){
                res.get(i).add(res_local.get(j));
            }

            // Update timer
            compute_distance_time += end_time_compute_dis - start_time_compute_dis;
            collect_node_time += end_time_collect_node - start_time_collect_node;
        }

        // Compute he new class for each target node by given top k neighbor nodes
        for(int i = 0; i < test_size; i ++){
            getNodeNewClassByTopKNeighbors(group.get(0).get(i), res.get(i).toArray(new Node[res.get(i).size()]));
        }

        // Calculate the correctness
        double acc = evaluateKnnCorrectness(group.get(0).toArray(new Node[group.get(0).size()]));
        // Stop the timer
        long end_time_all = System.currentTimeMillis();


        // Optional to write result to file
        if(WRITE_RESULT_TO_FILE > 0){
            writeKnnResultToFile(group.get(0).toArray(new Node[group.get(0).size()]), k);
        }
        if(WRITE_RESULT_DETAILS_TO_FILE > 0){
            writeKnnResultDetailsToFile(group.get(0), res, test_size, k);
        }


        System.out.println("Accuracy: " + acc);
        System.out.println("Elapsed time (Total) = " + (end_time_all - start_time_all));
        System.out.println("Elapsed time (Compute Distance) = " + compute_distance_time);
        System.out.println("Elapsed time (Collect Node) = " + collect_node_time);

        MASS.finish();
    }
}