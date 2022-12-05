/*
 * -------------------------------------------------------------------------------------------------
 * testMPI.java
 * Term: Autumn 2022
 * Class: CSS 534 – Parallel Programming In Grid And Cloud
 * HW: Program 5 - Final Project
 * Author: Warren Liu
 * Date: 12/03/2022
 * -------------------------------------------------------------------------------------------------
 * KNN Graph in parallel programming.
 * Part 1: Parallel in MPI.
 * Version 2, changed knn algorithm and input file structure.
 */

import mpi.*;
import java.io.*;
import java.util.*;
import java.lang.Math;


class knnJavaMPI_v2{

    /*
        Class to store global variables.
    */
    public static class Global {
        public static int TOP_K_BASE = 3;
        public static int TOP_K_TOP = 10;
        public static int MASTER = 0;

        
        public static int MPI_TAG_SEND_TEST_GROUP_SIZE = 101;
        public static int MPI_TAG_SEND_TRAIN_GROUP_SIZE = 102;
        public static int MPI_TAG_SEND_CHUNK_SIZE = 103;

        public static int MPI_TAG_SEND_TEST_GROUP = 201;
        public static int MPI_TAG_SEND_TRAIN_GROUP = 202;

        public static int MPI_TAG_SEND_BACK_TOP_K = 301;

    }


    /*
        Class Node: to store every knn node.
        double x: x-coordinate
        double y: y-coordinate
        double distance_to_target: distance to the target node
    */
    public static class Node implements Serializable, Comparable<Node>{
        double x;
        double y;
        double z;
        String className;
        String newClassName;
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

        public double getDistance(){
            return this.distance_to_target;
        }

        public String getClassName(){
            return this.className;
        }

        public String getResultForValidation(){
            return (this.x + "," + this.y + "," + this.x + "," + this.newClassName);
        }

        public void setNewClassName(String newClassName){
            this.newClassName = newClassName;
        }

        // Override print, to print node details
        @Override
        public String toString(){
            return ("Node [x=" + this.x + ", y=" + this.y + ", z=" + this.z + 
            ", class=" + this.className + ", newClass=" + this.newClassName + ", distance=" + this.distance_to_target + "]");
        }


        @Override
        public int compareTo(Node node){
            return (int) (this.distance_to_target - node.getDistance());
        }
    }

    public static double evaluateKnnCorrectness(Node[] nodeArr){
        double correct_count = 0.0;
        double wrong_count = 0.0;

        for(int i = 0; i < nodeArr.length; i ++){
            System.out.println(nodeArr[i].newClassName + " " + nodeArr[i].className);
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
        Function to calculate the Euclidean Distance between three nodes.

        Inputs:
            Node a: the first node
            Node b: the second node
        
        Returns:
            double distance: the Euclidean Distance between two nodes
    */
    public static double distance(Node a, Node b){
        double x = a.x - b.x;
        double y = a.y - b.y;
        double z = a.z - b.z;
        return Math.sqrt(x*x + y*y + z*z);
    }


    /*
        Helper function to sort Node array by nodes' distance_to_target.
    */
    public static class SortByNodeDistance implements Comparator<Node> {
        public int compare(Node a, Node b){
            return a.distance_to_target < b.distance_to_target ? -1 : 1;
        }
    }


    public static void getNodeNewClassByTopKNeighbors(Node node, Node[] top_k_neighbor_arr){
        for(Node n: top_k_neighbor_arr){
            System.out.println(n);
        }

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
        for(java.util.Map.Entry<String, Integer> pair: sortedCount.entrySet()){
            System.out.print(pair.getKey() + " " + pair.getValue() + ", ");
        }
        System.out.println();

        // Get max-vote. If votes are equal, use alphabet-order
        int max_vote = 0;
        String max_vote_class_name = "";
        for(java.util.Map.Entry<String, Integer> pair: sortedCount.entrySet()){
            if(pair.getValue() > max_vote){
                max_vote = pair.getValue();
                max_vote_class_name = pair.getKey();
            }
        }

        // System.out.println(max_vote + " " + max_vote_class_name);
        node.setNewClassName(max_vote_class_name);
    }


    static public void main(String[] args) throws MPIException, IOException{
        // Read all args
        // Validate args length
        if(args.length < 2){
            System.err.println("Usage: mpirun -n <node#> java knnJavaMPI_v2 <input_test_group_file> <input_train_group_file> <top_k> <optional: output_info>");
            System.err.println("top_K: an integer. otuput_info: 1 or 0 (default), if 1 is entered, all information will be display while running.");
            System.exit(-1);
        }

        // Init
        int k = Integer.parseInt(args[2]);
        // Input file for two groups of nodes
        File input_file_test = null;
        File input_file_train = null;
        // Scanner to read file
        Scanner sc_test = null;
        Scanner sc_train = null;

        Integer total_train_nodes = 0;
        Integer total_train_nodes_remainder = 0;
        Integer total_test_nodes = 0;
        Node[] train_group;
        Node[] test_group;
        // Node[][] top_k_gather_arr;
        Node[][] top_k_local_arr;

        int chunk_size = 0;
        if(args.length > 2){
            Global.TOP_K_BASE = Integer.parseInt(args[2]);
            Global.TOP_K_TOP = Integer.parseInt(args[2]) + 1;
        }

        // Start MPI
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        // Timer
        long start_time_all = System.currentTimeMillis();
        long start_time_read_file = System.currentTimeMillis();

        // Rank 0 reads file
        if(rank == 0){
            try{
                // Read test group
                input_file_test = new File(args[0]);
                sc_test = new Scanner(input_file_test);
    
                total_test_nodes = sc_test.nextInt();
                sc_test.nextLine();
                System.out.println("Rank[" + rank + "] - " + "Total test nodes: " + total_test_nodes);
                
                // Read train group
                input_file_train = new File(args[1]);
                sc_train = new Scanner(input_file_train);
    
                total_train_nodes = sc_train.nextInt();
                sc_train.nextLine();
                System.out.println("Rank[" + rank + "] - " + "Total test nodes: " + total_train_nodes);
    
                
                chunk_size = total_train_nodes / size;
                total_train_nodes_remainder = total_train_nodes % size;
                
            }
            catch(FileNotFoundException e){
                System.err.println("Cannot open input file: " + e);
                System.exit(-1);
            }
        }

        // Mater send size info to slaves
        if(rank == 0){
            int[] mpi_total_train_nodes = new int[1];
            mpi_total_train_nodes[0] = total_train_nodes;
            int[] mpi_total_test_nodes = new int[1];
            mpi_total_test_nodes[0] = total_test_nodes;
            int[] mpi_chunk_size = new int[1];
            mpi_chunk_size[0] = chunk_size;

            for(int i = 1; i < size; i ++){
                MPI.COMM_WORLD.Isend(
                    mpi_total_train_nodes,
                    0,
                    1,
                    MPI.INT,
                    i,
                    Global.MPI_TAG_SEND_TRAIN_GROUP_SIZE
                );
                MPI.COMM_WORLD.Isend(
                    mpi_total_test_nodes,
                    0,
                    1,
                    MPI.INT,
                    i,
                    Global.MPI_TAG_SEND_TEST_GROUP_SIZE
                );
                MPI.COMM_WORLD.Isend(
                    mpi_chunk_size,
                    0,
                    1,
                    MPI.INT,
                    i,
                    Global.MPI_TAG_SEND_CHUNK_SIZE
                );
                System.out.println("[Rank " + rank + "] - Send data to rank: " + i);
            }
        }
        else{
            int[] mpi_total_train_nodes = new int [1];
            int[] mpi_total_test_nodes = new int [1];
            int[] mpi_chunk_size = new int[1];

            MPI.COMM_WORLD.Recv(
                mpi_total_train_nodes,
                0,
                1,
                MPI.INT,
                0,
                Global.MPI_TAG_SEND_TRAIN_GROUP_SIZE
            );
            MPI.COMM_WORLD.Recv(
                mpi_total_test_nodes,
                0,
                1,
                MPI.INT,
                0,
                Global.MPI_TAG_SEND_TEST_GROUP_SIZE
            );
            MPI.COMM_WORLD.Recv(
                mpi_chunk_size,
                0,
                1,
                MPI.INT,
                0,
                Global.MPI_TAG_SEND_CHUNK_SIZE
            );

            total_train_nodes = mpi_total_train_nodes[0];
            total_test_nodes = mpi_total_test_nodes[0];
            chunk_size = mpi_chunk_size[0];
            // System.out.println("[Rank " + rank + "] - Received group size from Master.");
        }

        System.out.println("[Rank " + rank + "] - #TestNode: " + total_test_nodes + ", #TrainNode: " + total_train_nodes + ", chunkSize: " + chunk_size);
        
        //
        train_group = new Node[total_train_nodes];
        test_group = new Node[total_test_nodes];
        // top_k_gather_arr = new Node[total_test_nodes][size * k];
        ArrayList<ArrayList<Node>> top_k_gather_arr = new ArrayList<ArrayList<Node>>();
        for(int i = 0; i < total_test_nodes; i ++){
            top_k_gather_arr.add(new ArrayList<Node>());
        }

        top_k_local_arr = new Node[total_test_nodes][k];
        
        if (rank == 0){
            // Continue read test file
            int test_node_count = 0;
            try{
                while(sc_test.hasNextLine()){
                    String[] line = sc_test.nextLine().split(",", -1);
                    Node node = new Node(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), line[3]);
                    test_group[test_node_count ++] = node;
                }
            }
            catch(Exception e){
                System.err.println("Error while reading test file: " + e);
            }

            // for(int i = 0; i < test_group.size(); i ++){
            //     System.out.println(test_group.get(i));
            // }

            // Continue read train file
            try{
                int train_node_count = 0;
                while(sc_train.hasNextLine()){
                    String[] line = sc_train.nextLine().split(",", -1);
                    Node node = new Node(Double.parseDouble(line[0]), Double.parseDouble(line[1]), Double.parseDouble(line[2]), line[3]);
                    train_group[train_node_count ++] = node;
                }
            }
            catch(Exception e){
                System.err.println("Error while reading train file: " + e);
            }

            // Distribute data to slaves
            for(int i = 1; i < size; i ++){
                // Send test nodes to all slaves
                MPI.COMM_WORLD.Isend(
                    test_group,
                    0,
                    total_test_nodes,
                    MPI.OBJECT,
                    i,
                    Global.MPI_TAG_SEND_TEST_GROUP
                );

                // Send portion of train nodes to all slaves
                MPI.COMM_WORLD.Isend(
                    train_group,
                    chunk_size * (i - 1),
                    chunk_size,
                    MPI.OBJECT,
                    i,
                    Global.MPI_TAG_SEND_TRAIN_GROUP
                );
            }
        }
        // Other ranks receive from master
        else{
            MPI.COMM_WORLD.Recv(
                test_group,
                0,
                total_test_nodes,
                MPI.OBJECT,
                0,
                Global.MPI_TAG_SEND_TEST_GROUP
            );
            // System.out.println("Rank " + rank + " Successfully received array portion from Master");
            // for(int i = 0; i < test_group.length; i ++){
            //     System.out.println(test_group[i]);
            // }
            MPI.COMM_WORLD.Recv(
                train_group,
                chunk_size * (rank - 1),
                chunk_size,
                MPI.OBJECT,
                0,
                Global.MPI_TAG_SEND_TRAIN_GROUP
            );
        }

        // Stop the reading file timer
        long end_time_read_file = System.currentTimeMillis();
        
        // if(rank != Global.MASTER){
        //     for(int i = 0; i < train_group.length; i ++){
        //         if(train_group[i] != null)
        //             System.out.println(train_group[i]);
        //     }
        // }

        // Now Every rank has now received it's portion

        // Start KNN
        int start = 0, end = 0;
        if(rank == Global.MASTER){
            start = chunk_size * (size - 1);
            end = total_train_nodes;
        }
        else{
            start = chunk_size * (rank - 1);
            end = chunk_size * rank;
        }

        // For each node in test group
        for(int i = 0; i < total_test_nodes; i ++){
            ArrayList<Node> temp = new ArrayList<Node>();
            // For each node in train group
            for(int j = start; j < end; j ++){
                // Get the train node's distance to target
                temp.add(new Node(train_group[j].x, train_group[j].y, train_group[j].z, train_group[j].className, distance(test_group[i], train_group[j])));
            }

            // for(Node n: temp){
            //     System.out.println(n);
            // }

            // After calculating is done for the current target node
            // Sort the train group by distance
            Collections.sort(temp,  Comparator.comparing(Node::getDistance));
            // Store the top k neighbors
            for(int z = 0; z < k; z ++){
                if(rank != Global.MASTER){
                    top_k_local_arr[i][z] = temp.get(z);
                } 
                else{
                    // top_k_gather_arr[i][z] = temp.get(z);
                    top_k_gather_arr.get(i).add(temp.get(z));
                }
                // if(rank == 0) System.out.print(top_k_local_arr[i][z].getDistance() + " ");
            }
            // if(rank == 0) System.out.println();
        }

        // if(rank == 0){
        //     for(int i = 0; i < total_test_nodes; i ++){
        //         for(int j = 0; j < k; j ++){
        //             System.out.print(top_k_local_arr[i][j].getDistance() + " ");
        //         }
        //         System.out.println();
        //     }
        // }

        // Now everyone has get the top K for each target node in test group
        // Send this info back to Master
        if(rank == Global.MASTER){
            for(int i = 1; i < size; i ++){
                Node[][] temp = new Node[total_test_nodes][k];
                MPI.COMM_WORLD.Recv(
                    temp,
                    0,
                    total_test_nodes,
                    MPI.OBJECT,
                    i,
                    Global.MPI_TAG_SEND_BACK_TOP_K
                );

                // System.out.println("Received from rank: " + i);
                // for(int n = 0; n < total_test_nodes; n ++){
                //     for(int m = 0; m < k; m ++){
                //         System.out.print(temp[n][m].getDistance() + " ");
                //     }
                //     System.out.println();
                // }

                // Store received data
                for(int n = 0; n < total_test_nodes; n ++){
                    for(int m = 0; m < k; m ++){
                        // top_k_gather_arr[n][i * k + m] = temp[n][m];
                        top_k_gather_arr.get(n).add(temp[n][m]);
                    }
                }
            }
        }
        else{
            MPI.COMM_WORLD.Send(
                top_k_local_arr,
                0,
                total_test_nodes,
                MPI.OBJECT,
                Global.MASTER,
                Global.MPI_TAG_SEND_BACK_TOP_K
            );
        }

        if(rank == 0){
            
            for(int i = 0; i < total_test_nodes; i ++){
                Collections.sort(top_k_gather_arr.get(i),  Comparator.comparing(Node::getDistance));
                for(int j = 0; j < k; j ++){
                    top_k_local_arr[i][j] = top_k_gather_arr.get(i).get(j);
                }
            }

            // for(int i = 0; i < total_test_nodes; i ++){
            //     for(int j = 0; j < k * size; j ++){
            //         // System.out.print(top_k_gather_arr[i][j].getDistance() + " ");
            //         System.out.print(top_k_gather_arr.get(i).get(j).getDistance() + " ");
            //     }
            //     System.out.println();
            // }

            for(int i = 0; i < total_test_nodes; i ++){
                getNodeNewClassByTopKNeighbors(test_group[i], top_k_local_arr[i]);
                // for(int j = 0; j < k; j ++){
                //     System.out.print(top_k_local_arr[i][j].getDistance() + " ");
                // }
                // System.out.println();
            }

            double acc = evaluateKnnCorrectness(test_group);
            System.out.println("Accuracy: " + acc);
        }
        

        // // Sort local array
        // Collections.sort(neighbors_node_arr, Comparator.comparing(Node::getDistance));
        // // for(Node n: neighbors_node_arr){
        // //     System.out.println("Rank " + rank + ": " + n);
        // // }

        // if(rank == 0){
        //     for(int i = 0; i < Global.top_k; i ++){
        //         top_k_gather_arr[i] = neighbors_node_arr.get(i);
        //         // System.out.println("Rank " + rank + ": " + top_k_arr[i]);
        //     }
        // }
        // else{
        //     for(int i = 0; i < Global.top_k; i ++){
        //         top_k_arr[i] = neighbors_node_arr.get(i);
        //         // System.out.println("Rank " + rank + ": " + top_k_arr[i]);
        //     }
        // }
        

        // // System.out.println("Rank " + rank + " size of top_k_arr: " + top_k_arr.length);

        // // MPI.COMM_WORLD.Barrier();

        // // Send back to master
        // if(rank == 0){
        //     for(int i = 1; i < size; i ++){
        //         MPI.COMM_WORLD.Recv(
        //             top_k_gather_arr,
        //             i * Global.top_k,
        //             Global.top_k,
        //             MPI.OBJECT,
        //             i,
        //             MPI_TAG_SEND_BACK_RES
        //         );
        //         System.out.println("Master received rank " + i + " data back.");
        //     }

        //     // for(int i = 0; i < Global.top_k * size; i ++){
        //     //     System.out.println(top_k_gather_arr[i]);
        //     // }

        //     // Master get top k
        //     Arrays.sort(top_k_gather_arr, new SortByNodeDistance());
        //     for(int i = 0; i < Global.top_k; i ++){
        //         System.out.println("Top " + (i + 1) + ": " + top_k_gather_arr[i]);
        //     }

        //     long end_time_all = System.currentTimeMillis();
        //     System.out.println("Total elapsed time = " + (end_time_all - start_time_all));
        //     System.out.println("Total elapsed time for reading file = " + (end_time_read_file - start_time_read_file));
        //     System.out.println("Total elapsed time excepted file-reading = " + ((end_time_all - start_time_all) - (end_time_read_file - start_time_read_file)));
        // }
        // else{
        //     MPI.COMM_WORLD.Send(
        //         top_k_arr,
        //         0,
        //         Global.top_k,
        //         MPI.OBJECT,
        //         0,
        //         MPI_TAG_SEND_BACK_RES
        //     );
        //     System.out.println("Rank " + rank + " has sent back result.");
        // }


        // MPI.COMM_WORLD.Barrier();
        MPI.Finalize();
    }
}