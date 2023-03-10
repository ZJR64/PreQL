package src.Commands;

import src.StorageManager.StorageManager;

/**
 * The class for the DropTable Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class DropTable extends Command{

    // name of the table to drop
    private String name;

    private StorageManager sm;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public DropTable(String input, StorageManager sm) {
        super(input);
        this.sm = sm;
    }

    @Override
    public void parse() {
        try{
            String[] splitInput = input.split("(?i)table");
            this.name = splitInput[1].strip().replace(";", "");
            this.success = true;
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        return sm.dropTable(name);
    }
}
