/**
 * The main class for the database, recieves the arguments for creating a new database.
 *
 * @author Zachary Rutherford
 */
public class Main {
    String helpMessage = "java Main <db loc> <page size> <buffer size>";
    /**
     * The main method.  It checks the number of command line arguments,
     * then runs the database.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) {
        if (checkArgs(args)) {
            //create database
        }
        else {
            //print help message
            System.out.println(helpMessage)
        }
    }

    /**
     * A method that checks the command line arguments for correctness.
     * Displays the corresponding errors if not correct.
     *
     * @param args the command line arguments.
     */
    private static boolean checkArgs(String[] args) {
        //TODO
    }
}