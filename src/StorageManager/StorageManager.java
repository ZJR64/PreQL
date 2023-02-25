package src.StorageManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import src.Catalog.*;


/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    private static BufferManager bm;
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
           return "SUCCESS";
        }
        String path = table.getPath();


        return null;
    }

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
     * Gets all records for a given table number.
     */
    public void getAllRecords(String table){

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
}
