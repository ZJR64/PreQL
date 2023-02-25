package src.StorageManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
            int page_num = bm.pageSize * i;
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
    private String getAllRecords(String tableName){
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }
        if(table.getPages() == 0){
            return makeAttributesString(table).concat("\nSUCCESS");
        }
        for(int i = 0; i < table.getPages(); i++){
            int pageNum = bm.pageSize * i;
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

        Schema new_table = new Schema(name, null, attributes);
        c.schemas.add(new_table);
        return "Success";
    }
}
