package src.Commands;

import src.StorageManager.Record;
import src.StorageManager.StorageManager;

import java.util.ArrayList;

public class Delete extends Command{

    // table name
    String name;
    // where clause
    // null if no where clause
    WhereClause where;
    // storage manager
    StorageManager storageManager;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public Delete(String input, StorageManager storageManager) {
        super(input);
        this.storageManager = storageManager;
    }

    @Override
    public void parse() {
        try {
            // split on keyword from
            // should get [delete, name...]
            // only need second half of it
            String splitInput = input.split("(?i)from")[1];
            // if there's no where clause
            if (!splitInput.toLowerCase().contains("where")) {
                this.name = splitInput.replace(";", "").strip();
                this.where = null;
                this.success = true;
            }
            // otherwise, there's a where clause
            else {
                // split on the where
                String[] split = splitInput.split("(?i)where");
                // first half is table name
                this.name = split[0].strip();
                // second half is where clause
                this.where = new WhereClause(split[1].replace(";", "").strip());
                this.success = true;
            }
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        //TODO hook up to where
        ArrayList<Record> records = storageManager.getRecords(name, where);
        return storageManager.deleteRecords(records, name);
    }
}
