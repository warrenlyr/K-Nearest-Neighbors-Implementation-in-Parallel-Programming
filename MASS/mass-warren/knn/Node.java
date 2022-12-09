package knn;

import java.io.*;

public class Node implements Serializable, Comparable<Node>{
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