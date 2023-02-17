package src.Commands;
import src.Catalog.Catalog;
import src.Catalog.Schema;

/**
 * The class for the Display Info Command. It takes cat as an argument and calls on it
 * to search for the correct schema. It then uses the retrieved schema to display information to the user.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class DisplayInfo extends Command {
    String name;
    Schema schema;


    /**
     * Constructor for the DisplayInfo object
     *
     * @param input the entire input from the user.
     * @param cat the catalog for the database.
     */
    public DisplayInfo(String input, Catalog cat) {
        super(input);
        this.schema = cat.getSchema(name);
    }

    @Override
    public void parse() {
        //TODO currently only works with good user input
        String[] parts = input.split(" ");
        this.name = parts[2];
    }

    /**
     * Method used to execute the action of the command
     *
     * @return the status of the command, SUCCESS if successful, ERROR if not.
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