package src.Commands;
import src.Catalog.Catalog;

/**
 * The class for the Display Schema Command. It takes cat as an argument and calls on it
 * to print the necessary information to the user. Though not a complex or fleshed-out
 * class, it is necessary for maintaining the pattern of commands being granted their own
 * classes.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class DisplaySchema extends Command {
    String location;
    int pageSize;
    int bufferSize;
    Catalog cat;

    /**
     * Constructor for the DisplaySchema object
     *
     * @param input the entire input from the user.
     * @param location the database location
     * @param pageSize the page size for the database
     * @param bufferSize the buffer size for the database
     * @param cat the catalog for the database.
     */
    public DisplaySchema(String input, String location, int pageSize, int bufferSize, Catalog cat) {
        super(input);
        this.location = location;
        this.pageSize = pageSize;
        this.bufferSize = bufferSize;
        this.cat = cat;
    }

    @Override
    public void parse() {
        this.success = true;
        // nothing to parse
        return;
    }

    /**
     * Method used to execute the action of the command
     *
     * @return the status of the command, SUCCESS if successful, ERROR if not.
     */
    @Override
    public String execute() {
        System.out.println("DB location: " + location);
        System.out.println("Page Size: " + pageSize);
        System.out.println("Buffer Size: " + bufferSize);
        System.out.println(cat.toString());
        return "SUCCESS";
    }
}
