package knn;

import edu.uw.bothell.css.dsl.MASS.Place;

public class TrainGroup extends Place{
    public static final int init_ = 0;
    public static final int computeDistance_ = 1;
    // public static final int exchangeDistance_ = 2;
    public static final int collectDistance_ = 3;

    private Node node;

    public TrainGroup(Object args) {}

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
        System.out.println("here?1");
        this.node = (Node) args;
        System.out.println("here?2");
        return null;
    }

    public Object computeDistance(Object args){
        this.node.distance_to_target = this.node.distance((Node) args);
        return null;
    }

    public Double collectDistance(Object args){
        return this.node.distance_to_target;
    }
}