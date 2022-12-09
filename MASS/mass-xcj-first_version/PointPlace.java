package edu.uwb.xi7;

import edu.uw.bothell.css.dsl.MASS.Place;
import java.util.ArrayList;

public class PointPlace extends Place {
    public double x;
    public double y;
	public double z;
	public int classNum;
	public double distance;

	public final static int init = 0;
	public final static int dist = 1;
	public final static int collect = 2;
	
    public PointPlace(Object o) {}

    // public Point(Point p) {
    //     this.x = p.x;
    //     this.y = p.y;
    //     this.z = p.z;
    // }
	
	/**
	 * This method is called when "callAll" is invoked from the master node
	 */
	public Object callMethod(int method, Object o) {
		switch (method) {
			case 0:
				return init(o);
			case 1:
				return dist(o);
			case 2:
				return collect(o);
		}
		return null;
	}
	
	/** 
	 * Since size[] and index[] are not yet set by
	 * the system when the constructor is called, this init( ) method must
	 * be called "after" rather than "during" the constructor call
	 * @param args formally declared but actually not used
	 */
	public Object init(Object arg) {
		Point temp = (Point)arg;
		x = temp.x;
		y = temp.y;
		z = temp.z;
		classNum = temp.classNum;
		distance = 0;
		return null;
	}
	

	public Object dist(Object arg) {
		Point temp = (Point)arg;
		distance = temp.dist(x, y, z);
		return null;
	}
	
	public Double collect(Object arg) {
		return distance;
	}
}

