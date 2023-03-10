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
                //check if first page
                if(table.getPages() == 0){
                    bm.addPage(fileName, table.getOpenPages());
                    Page pg = new Page(0, table, bm.getPageSize(), 0);
                    bm.writePage(fileName, 0, pg.getBytes());
                }
                ArrayList<Integer> pgOrder = table.getPageOrder();
                //create record
                Record rec = new Record(table, attributes);
                //iterate through pages
                for (Integer i : pgOrder) {

                    //get page from buffer
                    Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                    //check if belongs
                    if (pg.belongs(rec.getPrimaryKey())) {
                        //add to page if belongs
                        if (!pg.addRecord(rec)) {
                            //split page if false
                            pg.split(bm, rec);
                        }
                        //write to buffer
                        bm.writePage(fileName, i, pg.getBytes());

                        //break because record has been added
                        break;
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
                String str = "";
                for (Attribute att: table.getAttributes()) {
                    str += rec.getAttributes().get(att.getName()) + " ";
                }
                System.out.println(str + "\n");
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
        c.addSchema(new_table);
        return "SUCCESS";
    }

    /**
     * Attempts to drop the table from the database.
     *
     * @param tableName name of the table.
     * @return A string reporting success or failure.
     */
    public String dropTable(String tableName){
        Schema schema = c.getSchema(tableName);
        if (schema == null){
            return "Table "+ tableName +" could not be found" + "\n ERROR";
        }
        bm.purge(schema.getFileName());
        c.removeSchema(schema);
        return "Success";
    }


    /**
     * Attempts to alter a table.
     *
     * @param tableName The name of the table being altered.
     * @param alterType Specifies whether a drop or add alteration is occurring.
     * @param attributeName The name of the attribute being added or dropped.
     * @param attributeType Type of the attribute being added, null if attribute is being dropped.
     * @param defaultValue What the table will fill in all new entries with, null if drop occurring.
     * @return A string reporting success or failure.
     */
    public String alterTable(String tableName, String alterType,
                             String attributeName, String attributeType,
                             String defaultValue) {
        Schema schema = c.getSchema(tableName);
        if(schema == null){
            return "Table " + tableName + " could not be found" + "\n ERROR";
        }

        if(alterType.equals("add")){   // add
            return StorageManagerHelper.alterAdd(schema, attributeName,
                    attributeType, defaultValue, bm);
        }
        else{ // drop
            return StorageManagerHelper.alterDrop(schema, attributeName, bm);

        }
    }
}
