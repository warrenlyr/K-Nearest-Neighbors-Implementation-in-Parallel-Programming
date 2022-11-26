/*
 * -------------------------------------------------------------------------------------------------
 * testMPI.java
 * Term: Autumn 2022
 * Class: CSS 534 – Parallel Programming In Grid And Cloud
 * HW: Program 5 - Final Project
 * Author: Warren Liu
 * Date: 11/26/2022
 * -------------------------------------------------------------------------------------------------
 * KNN Graph in parallel programming.
 * Part 1: Parallel in MPI.
 */

import mpi.*;
import java.io.*;
import java.util.*;
import java.lang.Math;


class knnJavaMPI{

    /*
        Class to store global variables.
    */
    public static class Global {
        public static int top_k = 1;
    }


    /*
        Class Node: to store every knn node.
        double x: x-coordinate
        double y: y-coordinate
        double distance_to_target: distance to the target node
    */
    public static class Node implements Serializable{
        double x;
        double y;
        double distance_to_target;

        public Node(){
            x = 0.0;
            y = 0.0;
            distance_to_target = 0.0;
        }

        public Node(double x, double y, double distance_to_target){
            this.x = x;
            this.y = y;
            this.distance_to_target = distance_to_target;
        }

        public double getDistance(){
            return this.distance_to_target;
        }

        // Override print, to print node details
        @Override
        public String toString(){
            return "Node [x=" + this.x + ", y=" + this.y + ", distance=" + this.distance_to_target + "]";
        }
    }


    /*
        Function to calculate the Euclidean Distance between two nodes.

        Inputs:
            Node a: the first node
            Node b: the second node
        
        Returns:
            double distance: the Euclidean Distance between two nodes
    */
    public static double distance(Node a, Node b){
        double x = a.x - b.x;
        double y = a.y - b.y;
        return Math.sqrt(x*x + y*y);
    }


    /*
        Helper function to sort Node array by nodes' distance_to_target.
    */
    public static class SortByNodeDistance implements Comparator<Node> {
        public int compare(Node a, Node b){
            return a.distance_to_target < b.distance_to_target ? -1 : 1;
        }
    }


    static public void main(String[] args) throws MPIException, IOException{
        // Read all args
        // Validate args length
        if(args.length < 1){
            System.err.println("Usage: mpirun -n <node#> java testMPI <input_file_name> <top_k>");
            System.exit(-1);
        }

        // Init
        File input_file = null;
        Scanner sc = null;
        Integer total_neighbors = 0;
        double[][] neighbors_arr;
        double[] target_node = new double[2];
        int chunk_size = 0;
        if(args.length > 1){
            Global.top_k = Integer.parseInt(args[1]);
        }

        // Start MPI
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        int tag_send_arr = 1;
        int tag_send_back_res = 2;
        Node[] top_k_gather_arr = new Node[Global.top_k * size];
        Node[] top_k_arr = new Node[Global.top_k];

        long start_time_all = System.currentTimeMillis();
        long start_time_read_file = System.currentTimeMillis();
        // Read file
        try{
            String input_file_name = args[0];
            input_file = new File(input_file_name);
            sc = new Scanner(input_file);

            total_neighbors = sc.nextInt();
            sc.nextLine();
            String[] target_node_line = sc.nextLine().split(" ", -1);

            System.out.println("Total neighbors: " + total_neighbors);
            // System.out.println(target_node_line);

            target_node[0] = Double.parseDouble(target_node_line[0]);
            target_node[1] = Double.parseDouble(target_node_line[1]);
            // System.out.println(target_node[0] + " - " + target_node[1]);

            chunk_size = total_neighbors / size;
        }
        catch(FileNotFoundException e){
            System.err.println("Cannot open input file: " + e);
            System.exit(-1);
        }
        
        //
        neighbors_arr = new double[total_neighbors][2];
        Node target = new Node(target_node[0], target_node[1], 0.0);
        ArrayList<Node> neighbors_node_arr = new ArrayList<Node>();
        
        if (rank == 0){
            // Get file name
            String input_file_name = args[0];
            System.out.println("File name: " + input_file_name);
            System.out.println("Total rank#: " + size);

            // Read from file
            int cnt = 0;
            try{
                while(sc.hasNextLine()){
                    String[] line = sc.nextLine().split(" ", -1);
                    neighbors_arr[cnt][0] = Double.parseDouble(line[0]);
                    neighbors_arr[cnt ++][1] = Double.parseDouble(line[1]);
                }
            }
            catch(Exception e){
                System.err.println("Error while reading file: " + e);
            }

            // Distribute data to other ranks
            System.out.println("Chunk size: " + chunk_size);
            for(int i = 1; i < size; i ++){
                MPI.COMM_WORLD.Isend(
                    neighbors_arr,
                    chunk_size * i,
                    chunk_size,
                    MPI.OBJECT,
                    i,
                    tag_send_arr
                );
                System.out.println("Master sent array portion to rank " + i);
            }
        }
        else{
            MPI.COMM_WORLD.Recv(
                neighbors_arr,
                chunk_size * rank,
                chunk_size,
                MPI.OBJECT,
                0,
                tag_send_arr
            );
            System.out.println("Rank " + rank + " Successfully received array portion from Master");
        }

        long end_time_read_file = System.currentTimeMillis();
        // int a = 0;
        // for(int i = 0; i < total_neighbors; i ++){
        //     System.out.println(rank + " - " + i + ": " +neighbors_arr[i][0] + " - " + neighbors_arr[i][1]);
        // }

        // Every rank has now received it's portion
        for(int i = chunk_size * rank; i < chunk_size * (rank + 1); i ++){
            Node new_neighbor = new Node(neighbors_arr[i][0], neighbors_arr[i][1], 0.0);
            new_neighbor.distance_to_target = distance(target, new_neighbor);
            neighbors_node_arr.add(new_neighbor);
            // System.out.println(rank + " - " + i + ": " + new_neighbor.x + " " + new_neighbor.y + " " + new_neighbor.distance_to_target);
        }

        // Sort local array
        Collections.sort(neighbors_node_arr, Comparator.comparing(Node::getDistance));
        // for(Node n: neighbors_node_arr){
        //     System.out.println("Rank " + rank + ": " + n);
        // }

        if(rank == 0){
            for(int i = 0; i < Global.top_k; i ++){
                top_k_gather_arr[i] = neighbors_node_arr.get(i);
                // System.out.println("Rank " + rank + ": " + top_k_arr[i]);
            }
        }
        else{
            for(int i = 0; i < Global.top_k; i ++){
                top_k_arr[i] = neighbors_node_arr.get(i);
                // System.out.println("Rank " + rank + ": " + top_k_arr[i]);
            }
        }
        

        // System.out.println("Rank " + rank + " size of top_k_arr: " + top_k_arr.length);

        // MPI.COMM_WORLD.Barrier();

        // Send back to master
        if(rank == 0){
            for(int i = 1; i < size; i ++){
                MPI.COMM_WORLD.Recv(
                    top_k_gather_arr,
                    i * Global.top_k,
                    Global.top_k,
                    MPI.OBJECT,
                    i,
                    tag_send_back_res
                );
                System.out.println("Master received rank " + i + " data back.");
            }

            // for(int i = 0; i < Global.top_k * size; i ++){
            //     System.out.println(top_k_gather_arr[i]);
            // }

            // Master get top k
            Arrays.sort(top_k_gather_arr, new SortByNodeDistance());
            for(int i = 0; i < Global.top_k; i ++){
                System.out.println("Top " + (i + 1) + ": " + top_k_gather_arr[i]);
            }

            long end_time_all = System.currentTimeMillis();
            System.out.println("Total elapsed time = " + (end_time_all - start_time_all));
            System.out.println("Total elapsed time for reading file = " + (end_time_read_file - start_time_read_file));
            System.out.println("Total elapsed time excepted file-reading = " + ((end_time_all - start_time_all) - (end_time_read_file - start_time_read_file)));
        }
        else{
            MPI.COMM_WORLD.Send(
                top_k_arr,
                0,
                Global.top_k,
                MPI.OBJECT,
                0,
                tag_send_back_res
            );
            System.out.println("Rank " + rank + " has sent back result.");
        }


        // MPI.COMM_WORLD.Barrier();
        MPI.Finalize();
    }
}