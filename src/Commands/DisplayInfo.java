package src.Commands;
import src.Catalog.Catalog;
import src.Catalog.Schema;

/**
 * The class for the Display Schema Command. It takes cat as an argument and calls on it
 * to print the necessary information to the user. Though not a complex or fleshed-out
 * class, it is necessary for maintaining the pattern of commands being granted their own
 * classes.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class DisplayInfo extends Command {
    String name;
    Schema schema;


    /**
     * Constructor for the DisplaySchema object
     *
     * @param input the entire input from the user.
     * @param cat the catalog for the database.
     */
    public DisplayInfo(String input, Catalog cat) {
        super(input);
        //TODO currently only works with good user grammar
        String[] parts = input.split(" ");
        this.name = parts[2];
        this.schema = cat.getSchema(name);
    }

    /**
     * Method used to execute the action of the command
     */
    @Override
    public String execute() {
        //see if schema exists
        if (schema != null) {
            System.out.println("Table Name: " + name);
            System.out.println("Table Schema: \n" + schema.toString());
            return "SUCCESS";
        }
        else {
            System.out.println("No such table " + name);
            return "ERROR";
        }
    }
}