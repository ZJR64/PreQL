package src.Commands;
import src.Catalog.Catalog;
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
