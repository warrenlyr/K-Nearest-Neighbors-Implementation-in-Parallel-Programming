import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.io.FileOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;

public class AccuracyCalc {
    public static void main(String[] args) throws Exception {
        // assumes the mapreduce output path is part-00000, and testnode file path is test.csv
        try {
            BufferedReader mapReduceOutput = new BufferedReader(new FileReader("part-00000"));
            BufferedReader testNodeFile = new BufferedReader(new FileReader("test.csv"));
            Integer totalNum = Integer.valueOf(testNodeFile.readLine());
            HashMap<Integer, String> table = new HashMap<Integer, String>();
            for (int i = 0; i < totalNum; i++) {
                String line = mapReduceOutput.readLine();
                String[] arr = line.split(" "); // arr[1] = new class
                Integer testNodeNumber;
                if (arr[0].charAt(1) == '.') {
                    testNodeNumber = Integer.valueOf(arr[0].substring(0, 1));
                }
                else {
                    testNodeNumber = Integer.valueOf(arr[0].substring(0, 2));
                }
                table.put(testNodeNumber, arr[1]);
            }
            int correct = 0;
            File fout = new File("myout.csv");
            FileOutputStream fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (int i = 0; i < totalNum; i++) {
                String line = testNodeFile.readLine();
                String[] arr = line.split(","); // arr[3] = original class
                if (arr[3].equals(table.get(i))) {
                    correct++;
                }
                bw.write(arr[0] + "," + arr[1] + "," + arr[2] + "," + table.get(i));
                bw.newLine();
            }
            bw.close();
            System.out.println("Accuracy: " + (double)correct / (double)totalNum);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
