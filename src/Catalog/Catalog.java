package src.Catalog;

import src.Helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * The class for the catalog. The catalog, also known as the data dictionary, stores
 * the database's metadata. This means the catalog is important to ensure
 * the tables know their own structures and relations.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
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
            //assign values
            this.pageSize = pageSize;

            //TODO temp file that adds a random schema to prove it works.
            schemas.add(new Schema("Group~1024~56~integer 4 id primaryKey~varchar 20 name"));

        }
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
        String output = "\nTables:";
        if (!schemas.isEmpty()) {
            for (Schema schema : schemas) {
                output += "\n\n" + schema.toString();
            }
        }
        else {
            output += "No schemas yet.";
        }
        return output;
    }

    public ArrayList<Schema> getSchemas() {
        return schemas;
    }
    public Schema getSchema(String name) {
        for (Schema schema: schemas) {
            if (schema.getName().equalsIgnoreCase(name)) {
                return schema;
            }
        }
        return null;
    }
}
