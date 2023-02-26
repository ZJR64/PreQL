package src.StorageManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
     *
     * @param tableName
     * @param tuples
     * @return
     */
    public String insert(String tableName, ArrayList<ArrayList<String>> tuples) {
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
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

        return "ERROR";
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
    private String checkAttributes(Schema table,
                                   ArrayList<ArrayList<String>> tuples){
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        for(ArrayList<String> tuple: tuples){                // for each tuple
            if(tuple.size() > tableAttributes.size()){  // check if too many attributes
                return "row (" + tuple.toString()+ ")" +
                        "Too few attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString();
            }
            else if(tuple.size() < tableAttributes.size()){ //check if enough attributes
                return "row (" + tuple.toString()+ ")" +
                        "Too many attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString();
            }
            for(int i = 0; i < tableAttributes.size(); i++){ // for each attribute in the table
                Attribute attr = tableAttributes.get(i); // get table attribute i
                String type = attr.getType();            // get type of attribute i
                String val = tuple.get(i);
                if(type.contains("varchar")){
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() <= amount){
                        break;
                    }
                    else{
                        return "TOO BIG ERROR";
                    }

                }
                else if(type.contains("char")){
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() == amount){
                        break;
                    }
                    else{
                        return "WRONG SIZE error";
                    }
                }
            }
        }


        return null;
    }

    /**
     * checks if the passed in string is either int or double.
     * @param str
     * @return
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
        ByteArrayOutputStream mkBytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(mkBytes);
        for(ArrayList<String> list : tuples){
            for(String entry : list){
                try{
                    out.writeUTF(entry);
                }
                catch(Exception e){
                    System.err.println("Error: Could not write out bytes");
                    return null;
                }
            }
        }
        byte[] bytes = mkBytes.toByteArray();
        return bytes;
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
            return makeAttributesString(table).concat("\nSUCCESS"); //returns the attributes of the table
        }
        System.out.println(makeAttributesString(table));                //print out the attributes

        for(int i = 0; i < table.getPages(); i++){                      //for each page in order
            int pageNum = bm.getPageSize() * i;                         //pageNum is the number of bytes the page starts at
            byte[] page = bm.getPage(tableName, pageNum);               //gets the next page
            System.out.println(page);                                      //TODO test this print


            int numOfRecs = 0;                                          //Initialize the number of records
            int index = 0;                                              //Index of the page
            while (index < Integer.SIZE) {
                numOfRecs = (numOfRecs << 8) + (page[index] & 0xFF);    //turns the first couple bytes into the number of records
                index++;
            }
            for (int recordNum = 0; recordNum < numOfRecs; recordNum+= (Integer.SIZE * 2)){
                                                                        //iterates through each record of the current page
                int offset = 0;                                         //The offset of the record
                while (index < Integer.SIZE) {
                    offset = (offset << 8) + (page[index] & 0xFF);
                    index++;
                }
                int size = 0;                                           //The size of the record
                while (index < Integer.SIZE) {
                    size = (size << 8) + (page[index] & 0xFF);
                    index++;
                }
                String type = table.getAttributes().get(i).getType();   //get the type of the attribute
                if (type.equals("integer")) {
                    int intValue = 0;                                           //The size of the record
                    for (int byteNum = 0; byteNum < Integer.SIZE; byteNum++) {
                        intValue = (intValue << 8) + (page[offset + byteNum] & 0xFF);
                    }

                }
                else if (type.equals("double")) {
                    byte[] bytes = new byte[Double.SIZE];
                    double doubleValue = 0;                             //The size of the record
                    for (int byteNum = 0; byteNum < Double.SIZE; byteNum++) {
                        bytes[byteNum] = page[offset + byteNum];
                    }
                    doubleValue = ByteBuffer.wrap(bytes).getDouble();
                }
                else if (type.equals("boolean")) {
                    boolean boolValue = false;
                    int temp = 0;
                    temp = (page[offset] & 0xFF);
                    boolValue = Boolean.valueOf(Integer.toString(temp));
                }
                else if (type.startsWith("char")) {
                    int charAmount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    String outputString = "";
                    for (int byteNum = 0; byteNum < charAmount; byteNum++) {
                        outputString = outputString +  (char) page[offset + byteNum];
                    }
                }
                else if (type.startsWith("varchar")) {
                    String outputString = "";
                    for (int byteNum = 0; byteNum < size; byteNum++) {
                        outputString = outputString +  (char) page[offset + byteNum];
                    }
                }
            }

        }

        return "SUCCESS";
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
