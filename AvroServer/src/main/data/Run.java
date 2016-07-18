package main.data;

/**
 * Created by sunyulong on 16.7.15.
 *
 */
public class Run {
    public static void main(String [] args){
        Server server = new ServerImpl();
        server.run();
        System.out.println("Server is runuing...");
    }
}
