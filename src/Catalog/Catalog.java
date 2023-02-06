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
        }
        System.out.println(this.pageSize);
    }

    /**
     * Getter for page
     */
    public int getPageSize() {
        return pageSize;
    }

    public void shutDown(){
        //record page size
        try {
            FileOutputStream outputStream = new FileOutputStream(catPath);
            byte[] pageBytes = new byte[] {(byte)(pageSize >>> 24),  (byte)(pageSize >>> 16),  (byte)(pageSize >>> 8), (byte)pageSize};
            outputStream.write(pageBytes);
            outputStream.flush();
            //make sure schemas is null
            if (schemas != null) {
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
}
