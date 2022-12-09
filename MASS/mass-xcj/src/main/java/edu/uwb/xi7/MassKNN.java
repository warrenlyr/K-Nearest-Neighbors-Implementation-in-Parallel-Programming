package edu.uwb.xi7;

import java.util.Date;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.Comparator;
import java.io.Serializable;

import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;
import java.io.FileNotFoundException;

public class MassKNN {
	@SuppressWarnings("unused")		// some unused variables left behind for easy debugging
	public static void main(String[] args) throws FileNotFoundException{
        int k = Integer.parseInt(args[0]);
        // Points are stored as <"X Y","Class"> for groundTruth and result
        HashMap<String, String> groundTruth = new HashMap<>();
        HashMap<String, String> result = new HashMap<>();
        // targets contains the points to be classified
		ArrayList<Point> targets = new ArrayList<>();
		final String classNames[] = { "clear", "clouds", "rain" };

		ArrayList<Point> train_arr = new ArrayList<>();
		Scanner input = new Scanner(new File("train.csv"));
		while (input.hasNextLine()) {
			String line = input.nextLine();
			String[] words = line.split(",");
			int classNum;
			if(words[3].equals("clear")){
				classNum=0;
			}
			else if(words[3].equals("clouds")){
				classNum=1;
			}
			else{
				classNum=2;
			}
			train_arr.add(new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1]),
					Double.parseDouble(words[2]), classNum));
		}

		// Read test points
		input = new Scanner(new File("test.csv"));
		int numTest = input.nextInt();
		input.nextLine();
		for (int i = 0; i < numTest; i++) {
			String line = input.nextLine();
			String[] words = line.split(",");
			targets.add(new Point(Double.parseDouble(words[0]), Double.parseDouble(words[1]),
					Double.parseDouble(words[2]), -1));
			groundTruth.put(words[0] + " " + words[1] + " " + words[2], words[3]);
		}

		// Divide train points to ranks
		int n_places = 4;
		int bound = train_arr.size() / n_places;
		int rest = train_arr.size() % n_places;
		ArrayList<ArrayList<Point>> placeInputArr = new ArrayList<>();
		for (int i = 0; i < n_places; i++) {
			placeInputArr.add(new ArrayList<Point>());
			if (i < rest) {
				for (int j = 0; j < bound + 1; j++) {
					placeInputArr.get(i).add(new Point(train_arr.get(i * (bound + 1) + j)));
				}
			} else {
				for (int j = 0; j < bound; j++) {
					placeInputArr.get(i).add(new Point(train_arr.get(i * bound + rest + j)));
				}
			}
		}
		Object train[] = placeInputArr.toArray();

		// remember starting time
		long startTime = new Date().getTime();
		
		// init MASS library
		MASS.setNodeFilePath("nodes.xml");
		MASS.setLoggingLevel(LogLevel.ERROR);
		// start MASS
		MASS.init();
		
		// Create all places
		Places places = new Places(1, PointPlace.class.getName(), null, n_places);
		places.callAll(PointPlace.init, train);
		
		for (Point target : targets) {
			// Calculate distance
			places.callAll(PointPlace.dist, target);

			// (Object[])null is not a good interface design
			Object allDistances[]=places.callAll(PointPlace.collect, (Object[])null);
			
			ArrayList<ClassDist> distances = new ArrayList<>();

			int m = 0;
			int h = 0;
			for (int i = 0; i < allDistances.length; i++) {
				ArrayList<Double> temp = (ArrayList<Double>) allDistances[i];
				int placeSize = temp.size();
				h += placeSize;
				// System.out.println("train size / placeSize:  " + train_arr.size() + " " + h);
				for (int j = 0; j < placeSize; j++) {
					distances.add(new ClassDist(train_arr.get(m).classNum, temp.get(j)));
					m++;
				}
			}

			distances.sort(new DistComp());

			distances = new ArrayList<ClassDist>(distances.subList(0, k));

            // Do majority voting
            // Find max class number
            int maxClass = 0;
            for (int i = 0; i < k; i++) {
                if (distances.get(i).classNum > maxClass) {
                    maxClass = distances.get(i).classNum;
                }
            }
            // Vote
            int votes[] = new int[maxClass + 1];
            for (int i = 0; i < k; i++) {
                votes[distances.get(i).classNum] += 1;
            }
            // Find the winning class
            int winClass = 0;
            int maxVote = 0;
            for (int i = 0; i < maxClass + 1; i++) {
                if (votes[i] > maxVote) {
                    maxVote = votes[i];
                    winClass = i;
                }
            }

            result.put(target.x + " " + target.y + " " + target.z, classNames[winClass]);
		}

		MASS.finish();
		MASS.getLogger().debug( "MASS has stopped" );
		
        // Evaluate acc (accuracy)
        double acc=0;
        for (Entry<String,String> e : result.entrySet()) {
            if (groundTruth.get(e.getKey()).equals(e.getValue())) {
                acc++;
            }
        }
        acc /= numTest;
        System.out.println("acc: " + acc);

		// calculate / display execution time
		long execTime = new Date().getTime() - startTime;
		System.out.println( "Execution time = " + execTime + " ms" );
		
	 }
	 
}

// Comparator class for ClassDist
class DistComp implements Comparator<ClassDist>, Serializable {
    public int compare(ClassDist v1, ClassDist v2) {
        return Double.compare(v1.dist,v2.dist);
    }
}