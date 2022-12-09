package edu.uwb.xi7;

import java.io.Serializable;

public class Point implements Serializable{
    public double x;
    public double y;
    public double z;
    public int classNum;

    public Point(double x, double y, double z, int classNum) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.classNum = classNum;
    }

    public Point(Point p) {
        this.x = p.x;
        this.y = p.y;
        this.z = p.z;
        this.classNum = p.classNum;
    }

    public double dist(Point p2) {
        return Math.sqrt(Math.pow((this.x - p2.x), 2) + Math.pow((this.y - p2.y), 2) + Math.pow((this.z - p2.z), 2));
    }

    public double dist(double x, double y, double z) {
        return Math.sqrt(Math.pow((this.x - x), 2) + Math.pow((this.y - y), 2) + Math.pow((this.z - z), 2));
    }
}