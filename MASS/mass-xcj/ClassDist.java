package edu.uwb.xi7;

import java.io.Serializable;

public class ClassDist implements Serializable {
    public int classNum;
    Double dist;

    public ClassDist(int classNum, Double dist) {
        this.classNum = classNum;
        this.dist = dist;
    }
}