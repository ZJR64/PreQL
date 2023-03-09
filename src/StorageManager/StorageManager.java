package src.StorageManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import src.Catalog.*;


/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    public BufferManager bm;
    private Catalog c;

    public StorageManager(BufferManager bm, Catalog c){
        this.bm = bm;
        this.c = c;
    }

    /**
     * Inserts into a table a number of records.
     *
     * @param tableName table being inserted into
     * @param tuples the records to be inserted into the table.
     * @return A string reporting the success/failure of the command.
     */
    public String insert(String tableName, ArrayList<ArrayList<String>> tuples) {
        Schema table = c.getSchema(tableName);
        Map<String, Object> attributes;
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }
        String fileName = table.getFileName();

        for(ArrayList<String> tuple : tuples){  // for tuple in tuples, for loop will create tuples into records.
            attributes = StorageManagerHelper.checkAttributes(table, tuple, bm);
            if(attributes != null){ // we're good, the tuple is valid and we can make the record.
                if(table.getPages() == 0){
                    bm.addPage(fileName, table.getOpenPages());
                }
                else {
                    ArrayList<Integer> pgOrder = table.getPageOrder();
                    for (Integer i : pgOrder) {
                        Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                        Record rec = new Record(table, attributes);
                        if (!pg.addRecord(rec)) {
                            pg.split(bm, attributes); // Do I need to update values here like pgOrder? etc.
                        }
                    }
                }
            }
            else{
                return "\nERROR";
            }
        }
        return "SUCCESS";
    }


    /**
     * Gets a record by using its primary key.
     */
    public void getRecord(){

    }

    /**
     * Gets a page by it's table and page number.
     */
    public void getPage(){

    }

    /**
     * Gets all records from the given table.
     *
     * @param tableName The table the records are being gotten from.
     * @return A string reporting the success/failure of the command.
     */
    public String getAllRecords(String tableName){
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");  //returns an error if there is no table
        }
        if(table.getPages() == 0){
            return StorageManagerHelper.makeAttributesString(table).concat("\nSUCCESS"); //returns the attributes of the table
        }
        System.out.println(StorageManagerHelper.makeAttributesString(table));                //print out the attributes

        ArrayList<Integer> pageList = table.getPageOrder();
        for (Integer pgNum : pageList){
            Page pg = new Page(pgNum, table, bm.getPageSize(), bm.getPage(table.getFileName(), pgNum));
            for (Record rec : pg.getRecords()) {
                for (String att: rec.getAttributes().keySet()) {
                    System.out.println(rec.getAttributes().get(att) + "  |  ");
                }
                System.out.println("\n");
            }
        }

        return "SUCCESS";
    }


    /**
     * Deletes a record from a given table using the given primary key.
     */
    public void deleteRecord(){

    }

    /**
     * Updates a record in a given table using the given primary key.
     */
    public void updateRecord(){

    }

    /**
     * Attempts to create a new table in the database.
     *
     * @param name The name of the table being created.
     * @param attributes The list of attributes the table will have.
     * @return A string reporting success or failure.
     */
    public String createTable(String name, ArrayList<Attribute> attributes){
        for (Schema i : c.getSchemas()) {
            if(i.getName().equals(name)){
                return "Table already exists" +
                        "\nERROR";
            }
        }

        Schema new_table = new Schema(name, attributes);
        c.schemas.add(new_table);
        return "Success";
    }
}
