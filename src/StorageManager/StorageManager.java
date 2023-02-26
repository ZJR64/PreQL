package src.StorageManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.ArrayList;

import src.Catalog.*;
import src.Commands.CreateTable;


/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    public static BufferManager bm;
    private static Catalog c;

    public StorageManager(BufferManager bm, Catalog c){
        this.bm = bm;
        this.c = c;
    }

    /**
     * Inserts into a table a number of records.
     *
     * @param tableName The table being inserted into
     * @param tuples the records to be inserted into the table.
     * @return If the insertion was successful or not.
     */
    public String insert(String tableName, ArrayList<ArrayList<String>> tuples) {
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }
        ArrayList<Integer> result = checkAttributes(table, tuples);
        if(result == null){  // if result equals null, attributes are good.
            return "ERROR";
        }

        byte[] byteArray;
        //Make the byte array
        try{
            byteArray = convertToBytes(tuples);
            if(byteArray == null){
                System.err.println("Error: Could not write out bytes");
                return "FILE ERROR";
            }
        }
        catch(Exception e){
            System.err.println("Error: tuples could not be read");
            return "FILE ERROR";
        }
        //if table doesnt yet have pages.
        if(table.getPages() == 0){
           bm.addPage(tableName, byteArray);
           table.addPage();
           table.addRecord();
           return "SUCCESS";
        }

        //Read each table page in order from the table file.
        for(int i = 0; i < table.getPages(); i++){
            // page_num = 0 = first page, page_num = 1 second page, etc.
            int page_num = bm.getPageSize() * i;
            byte[] page = bm.getPage(tableName, page_num);

            byte[] bytes = new byte[Integer.SIZE];
                for (int j = 0; j < Integer.SIZE; j++) {
                    bytes[j] = page[j];
                }
                int value = 0;
                for (byte b : bytes) {
                    value = (value << 8) + (b & 0xFF);
                }
        }

        return null;
    }


    /**
     * Checks if the user is trying to insert the correct number and types of
     * values into the table.
     *
     * @param table the table that contains the attributes being checked against.
     * @param tuples the tuples containing the attributes being checked
     * @return null if the tuples are correct, otherwise a string explaining
     * what is wrong with the string.
     */
    private ArrayList<Integer> checkAttributes(Schema table,
                                   ArrayList<ArrayList<String>> tuples){
        ArrayList<Integer> attributeSizes = new ArrayList<>();
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        for(ArrayList<String> tuple: tuples){                // for each tuple
            if(tuple.size() > tableAttributes.size()){  // check if too many attributes
                 System.out.println("row (" + tuple.toString()+ ")" +
                        "Too few attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString());
            }
            else if(tuple.size() < tableAttributes.size()){ //check if enough attributes
                System.out.println("row (" + tuple.toString()+ ")" +
                        "Too many attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString());
            }
            for(int i = 0; i < tableAttributes.size(); i++){ // for each attribute in the table
                Attribute attr = tableAttributes.get(i); // get table attribute i
                String type = attr.getType();            // get type of attribute i
                String val = tuple.get(i);
                if(type.equals("varchar")){
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() <= amount){
                        attributeSizes.add(val.length());
                        continue;
                    }
                    else{
                         System.out.println("TOO BIG ERROR");
                    }

                }
                else if(type.equals("char")){
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() == amount){
                        attributeSizes.add(val.length());
                        continue;
                    }
                    else{
                        System.out.println("WRONG SIZE error");
                    }
                }
                else if(type.equals("double")){
                    if(isNumeric(val)){
                        if(val.contains(".")){
                            System.out.println("EXPECTED INTEGER, GOT DOUBLE");
                        }
                        else{
                            attributeSizes.add(Double.SIZE);
                            continue;
                        }
                    }
                    else{
                        System.out.println("EXPECTED DOUBLE, GOT NOT DOUBLE ERROR");
                    }
                }
                else if(type.equals("integer")){
                    if(isNumeric(val)){
                        if(val.contains(".")){
                            System.out.println("EXPECTED INTEGER, GOT DOUBLE");
                        }
                        else{
                            attributeSizes.add(Integer.SIZE);
                            continue;
                        }
                    }
                    else{
                        System.out.println("EXPECTED INTEGER, GOT NOT INTEGER ERROR");
                    }
                }
                else{
                    System.out.println("INVALID TYPE ERROR");
                }
            }
        }
        return null;
    }

    /**
     * checks if the passed in string is either int or double.
     * @param str the string being checked.
     * @return true if the string is numeric, false otherwise.
     */
    private Boolean isNumeric(String str){
        try {
            if(str.contains(".")){
                double val = Double.parseDouble(str);
            }
            else{
                int val = Integer.parseInt(str);
            }

        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    /**
     * Takes in a table and page number, gets the number of records in that
     * page.
     * @param tableName the tabble whose page's number of records is being
     *                  gotten from.
     * @param pageNum the page number whose number of records is being gotten
     *                from.
     * @return The number of records in the passed page.
     */
    private int getNumRecords(String tableName, int pageNum){
        byte[] first_page = bm.getPage(tableName, pageNum); // get page

        byte[] bytesNumRecs = new byte[Integer.SIZE]; // byte array for number of records

        for (int j = 0; j < Integer.SIZE; j++) {
            bytesNumRecs[j] = first_page[j]; // set each byte of numrecs to byte of page
        }
        int numRecs = 0;
        for (byte b : bytesNumRecs) {
            numRecs = (numRecs << 8) + (b & 0xFF); //convert bytes of num recs to actual value.
        }
        return numRecs;
    }

    /**
     * Converts the passed in tuples to a byte array.
     * @param tuples
     * @return
     */
    private static byte[] convertToBytes(ArrayList<ArrayList<String>> tuples){
        //WILL DO LATER
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
            return "No such table " + tableName.concat("\nERROR");
        }
        if(table.getPages() == 0){
            return makeAttributesString(table).concat("\nSUCCESS");
        }
        for(int i = 0; i < table.getPages(); i++){
            int pageNum = bm.getPageSize() * i;
            byte[] page = bm.getPage(tableName, pageNum);

        }

        return "ERROR";
    }


    /**
     * Makes a string containing nicely formatted attributes.
     * @param table The table the attributes are pulled from.
     * @return The completed attributes string.
     */
    public String makeAttributesString(Schema table){
        ArrayList<Attribute> attributes = table.getAttributes();
        StringBuilder str = new StringBuilder();
        str.append("\n");
        StringBuilder topStr = new StringBuilder();
        StringBuilder midStr = new StringBuilder();
        StringBuilder botStr = new StringBuilder();
        for(int i = 0; i < attributes.size(); i++){
            Attribute atr = attributes.get(i);
            String atrName = atr.getName();
            int atrSize = atrName.length();
            for(int j = 0; j < atrSize + 4; j++){ // attribute "size" would make
                topStr.append("-");                  // "--------"
            }
            String bottomBox = str.toString();
            if(i == 0){
                midStr.append("| ");
            }
            midStr.append(atrName);
            midStr.append(" | ");
         }
        botStr.append(topStr);
        str.append(topStr);
        str.append("\n");
        str.append(midStr);
        str.append("\n");
        str.append(botStr);
        return str.toString();
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
