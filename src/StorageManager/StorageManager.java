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
     * @return If the insertion was successful or not.
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
                ArrayList<Integer> pgOrder = table.getPageOrder();
                for(Integer i : pgOrder){
                    Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                    Record rec = new Record(table, attributes);
                    if(!pg.addRecord(rec)){
                        pg.split(bm, attributes); // Do I need to update values here like pgOrder? etc.
                    }
                    else{
                        return "SUCCESS";
                    }
                }
            }
            else{
                return "\nERROR";
            }
        }
        return null;
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
     *
     * @param tableName
     * @return
     */
    public String getAllRecords(String tableName){
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");  //returns an error if there is no table
        }
        if(table.getPages() == 0){
            return StorageManagerHelper.makeAttributesString(table).concat("\nSUCCESS"); //returns the attributes of the table
        }
        System.out.println(StorageManagerHelper.makeAttributesString(table));  //print out the attributes

        for(int i = 0; i < table.getPages(); i++){                      //for each page in order
            byte[] page = bm.getPage(tableName, i);                     //gets the next page
            System.out.println(page);                                      //TODO test this print


            int numOfRecs = 0;                                          //Initialize the number of records
            int index = 0;                                              //Index of the page
            while (index < Integer.SIZE/8) {
                numOfRecs = (numOfRecs << 8) + (page[index] & 0xFF);    //turns the first couple bytes into the number of records
                index++;
            }
            index += Integer.SIZE/8;
            for (int recordNum = 0; recordNum < numOfRecs; recordNum++){
                //iterates through each record of the current page
                String recordOutput = "| ";                             //Initialize the string of the record

                for (Attribute att:table.getAttributes()) {

                    int offset = 0;                                         //The offset of the
                    while (index < Integer.SIZE/8) {
                        offset = (offset << 8) + (page[index] & 0xFF);
                        index++;
                    }
                    int size = 0;                                           //The size of the record
                    while (index < Integer.SIZE/8) {
                        size = (size << 8) + (page[index] & 0xFF);
                        index++;
                    }
                    String type = att.getType();                                    //get the type of the attribute
                    if (type.equals("integer")) {
                        int intValue = 0;                                           //The size of the record
                        for (int byteNum = 0; byteNum < Integer.SIZE/8; byteNum++) {
                            intValue = (intValue << 8) + (page[offset + byteNum] & 0xFF);
                        }
                        String tempString = Integer.toString(intValue);
                        tempString = String.format("%1$"+15+ "s", tempString);
                        recordOutput = recordOutput + tempString + " | ";

                    }
                    else if (type.equals("double")) {
                        byte[] bytes = new byte[Double.SIZE/8];
                        double doubleValue = 0;                             //The size of the record
                        for (int byteNum = 0; byteNum < Double.SIZE/8; byteNum++) {
                            bytes[byteNum] = page[offset + byteNum];
                        }
                        int attributeSize = att.getSize();
                        doubleValue = ByteBuffer.wrap(bytes).getDouble();
                        String tempString = Double.toString(doubleValue);
                        tempString = String.format("%1$"+20+ "s", tempString);
                        recordOutput = recordOutput + tempString + " | ";

                    }
                    else if (type.equals("boolean")) {
                        boolean boolValue = false;
                        int temp = 0;
                        temp = (page[offset] & 0xFF);
                        boolValue = Boolean.valueOf(Integer.toString(temp));
                        recordOutput = recordOutput + boolValue + " | ";

                        if (boolValue == false){
                            recordOutput = recordOutput + "false | ";
                        }
                        else{
                            recordOutput = recordOutput + "true  | ";
                        }

                    }
                    else if (type.startsWith("char")) {
                        int charAmount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                        String outputString = "";
                        for (int byteNum = 0; byteNum < charAmount; byteNum++) {
                            outputString = outputString +  (char) page[offset + byteNum];
                        }
                        recordOutput = recordOutput + outputString + " | ";

                    }
                    else if (type.startsWith("varchar")) {
                        String outputString = "";
                        for (int byteNum = 0; byteNum < size; byteNum++) {
                            outputString = outputString +  (char) page[offset + byteNum];
                        }
                        recordOutput = recordOutput + outputString + " | ";

                    }
                }
                System.out.println(recordOutput + "\n");

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
