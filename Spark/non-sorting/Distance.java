import java.io.Serializable;

public class Distance implements Serializable{
    public double distance;
    public int classNum;

    public Distance(double distance, int classNum) {
        this.distance = distance;
        this.classNum = classNum;
    }
}