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


    public static double distance(Node a, Node b){
        double x = a.x - b.x;
        double y = a.y - b.y;
        double z = a.z - b.z;
        return Math.sqrt(x*x + y*y + z*z);
    }


    public class TrainGroup extends Place{
        public static final int init_ = 0;
        public static final int computeDistance_ = 1;
        // public static final int exchangeDistance_ = 2;
        public static final int collectDistance_ = 3;

        private Node node;

        public TrainGroup() {}

        public Object callMethod(int funcId, Object args){
            switch(funcId){
                case init_: return init(args);
                case computeDistance_: return computeDistance(args);
                // case exchangeDistance_: return exchangeDistance(arge);
                case collectDistance_: return collectDistance(args);
            }
            return null;
        }

        public Object init(Object args){
            this.node = (ArrayList<Node>) args;
            return null;
        }

        public Object computeDistance(Object args){
            this.node.distance_to_target = distance((Node) args, this.node);
            return null;
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

        MASS.init();

        Places knn_place = new Places(1, TrainGroup.class.getName(), null, train_size, 1);
        Object[] train_group = group.get(1).toArray();
        knn_place.callAll(TrainGroup.init_, train_group);

        // For each target node
        for(int i = 0; i < test_size; i ++){
            // Calculate each train node (in the matrix) distance to this target node
            knn_place.callAll(TrainGroup.computeDistance_, group.get(0).get(i));
            // Collect calculated distance from the matrix
            Object[] temp = knn_place.callAll(TrainGroup.collectDistance_, group.get(1).toArray());
            for(Object x: temp){
                System.out.println((Double) x);
            }
        }

        MASS.finish();
    }
}