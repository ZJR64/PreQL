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
           return "SUCCESS";
        }
        String path = table.getPath();



        return null;
    }

    /**
     *
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
            return "No such table " + tableName.concat("\nERROR");
        }
        String path = table.getPath();


        return "ERROR";
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
                return "ERROR";
            }
        }

        Schema new_table = new Schema(name, name + ".tbl", attributes);
        c.schemas.add(new_table);
        bm.addPage(new_table.getName(), null);
        return "Success";
    }
}
