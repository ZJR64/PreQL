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
public class DisplaySchema {

    /**
     * Constructor for the DisplaySchema object
     *
     * @param input the entire input from the user.
     * @param cat the catalog for the database.
     */
    public DisplaySchema(String input, Catalog cat) {
        System.out.println(cat.getSchema());
    }


}
