package src;

/**
 * The main class for the database, recieves the arguments for creating a new database.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Main {
    static String helpMessage = "java Main <db loc> <page size> <buffer size>";
    /**
     * The main method.  It checks the number of command line arguments,
     * then runs the database.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) {
        if (args.length == 4) {
            //check if directory exists
            if (!Helper.checkDir(args[0])) {
                System.err.println("Error: directory could not be created at " + args[0]);
                System.out.println(helpMessage);
                return;
            }
            //check if page size is a number.
            if (!Helper.isNum(args[1])) {
                System.err.println("Error: page_size must be a number");
                System.out.println(helpMessage);
                return;
            }
            //check if buffer size is a number.
            if (!Helper.isNum(args[2])) {
                System.err.println("Error: buffer_size must be a number");
                System.out.println(helpMessage);
                return;
            }
            boolean indexing = false;
            if(Helper.isTrue(args[3])){
                indexing = true;
            }

            //create the database
            Database db = new Database(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), indexing);
            db.run();
        }
        else {
            //print help message
            System.err.println("Error: incorrect number of command line arguments");
            System.out.println(helpMessage);
        }
    }
}