package src.Catalog;

import src.Helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Scanner;

public class Catalog {

    private String catPath;
    private int pageSize;
    private ArrayList<Schema> schemas;


    /**
     * Constructor for the Catalog object
     *
     * @param catPath the file path to the catalog.
     * @param pageSize the size of each page of the database.
     */
    public Catalog(String catPath, int pageSize) {
        this.schemas = new ArrayList<Schema>();
        this.catPath = catPath;
        if (Helper.checkFile(catPath)) {
            System.out.println("catalog found.");
            //make the catalog read from file
            byte[] byteArray;
            try {
                FileInputStream inputStream = new FileInputStream(catPath);;
                byteArray = inputStream.readAllBytes();
                inputStream.close();
            } catch (Exception e) {
                System.err.println("Error: catalog file could not be read");
                return;
            }
            //extract info
            byte[] pageSizeArray = {byteArray[0], byteArray[1], byteArray[2], byteArray[3]};
            ByteBuffer byteBuffer = ByteBuffer.wrap(pageSizeArray);
            String unfiltered = new String(byteArray);
            //get rid of page size
            unfiltered = unfiltered.substring(4);
            String[] filtered = unfiltered.split(";");
            //add schemas
            for (String s : filtered) {
                if (!s.equals("")) {
                    schemas.add(new Schema(s));
                }
            }
            this.pageSize = byteBuffer.getInt();
            byteBuffer.clear();

        } else {
            try {
                File newFile = new File(catPath);
                newFile.createNewFile();
                System.out.println("catalog file created.");
            } catch (Exception e) {
                System.err.println("Error: catalog file could not be created");
                return;
            }
            //assign values
            this.pageSize = pageSize;

            //TODO temp file that adds a random schema to prove it works.
            schemas.add(new Schema("Group integer group_id varchar name varchar role integer age"));

        }
        System.out.println(this.pageSize);
    }

    /**
     * Getter for page size
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Method that safely stores the Catalog when the database is shut down.
     */
    public void shutDown(){
        //record page size
        try {
            FileOutputStream outputStream = new FileOutputStream(catPath);
            byte[] pageBytes = new byte[] {(byte)(pageSize >>> 24),  (byte)(pageSize >>> 16),  (byte)(pageSize >>> 8), (byte)pageSize};
            outputStream.write(pageBytes);
            outputStream.flush();
            //make sure schemas is populated
            if (!schemas.isEmpty()) {
                for (Schema s : schemas) {
                    outputStream.write(s.writeable().getBytes());
                    outputStream.flush();
                }
            }
            outputStream.close();

        } catch (Exception e) {
            System.err.println("Error: catalog file could not be written to");
            System.err.println(e);
            return;
        }
    }

    /**
     * Converts the schemas into a printable form for the user.
     *
     * @return the string representing every schema in the catalog.
     */
    public String getSchema() {
        String output = "Database Schema:\n\n";
        if (!schemas.isEmpty()) {
            for (Schema s : schemas) {
                output += s.toString() + "\n";
            }
        }
        else {
            output += "No schemas yet.";
        }
        return output;
    }
}
