import mpi.*;
import java.io.*;
import java.util.*;

class testMPI{
    static public void main(String[] args) throws MPIException, IOException{

        // Start MPI
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        int number[] = {1,2,3,4};
        int[] number_2 = new int[4];

        if(rank == 0){
            for(int i = 1; i < size; i ++){
                MPI.COMM_WORLD.Send(
                    number, 0, 4, MPI.INT, i, 999
                );
                System.out.println("Seccessfully sent arr to rank " + i);
            }
        }
        else{
            MPI.COMM_WORLD.Recv(
                number_2, 0, 4, MPI.INT, 0, 999 
            );
            System.out.println("Seccessfully received arr from Mater:  " + rank);
            for(int i = 0; i < 4; i ++){
                System.out.println("Rank " + rank + " - " + i + ": " + number_2[i]);
            }
        }

        MPI.Finalize();
    }
}