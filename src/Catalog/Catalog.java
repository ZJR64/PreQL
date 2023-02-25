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
    public ArrayList<Schema> schemas;


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
            byte[] inverse = Helper.invertBits(byteArray);
            String unfiltered = new String(inverse);
            //get rid of page size
            String[] filtered = unfiltered.split(";");
            //add schemas
            for (String s : filtered) {
                if (!s.equals("")) {
                    schemas.add(new Schema(s));
                }
            }

        } /**else {

            //TODO temp line that adds random schema to prove it works.
            schemas.add(new Schema("Group~doesnotexist.txt~1024~56~integer 4 id primaryKey~varchar 20 name~double 8 gpa"));
            schemas.add(new Schema("School~hubris~209~5000~integer 4 id primaryKey~varchar 20 name~char 10 building"));
        }*/
    }

    /**
     * Method that safely stores the Catalog when the database is shut down.
     */
    public void shutDown(){
        //record page size
        try {
            FileOutputStream outputStream = new FileOutputStream(catPath);
            //make sure schemas is populated
            if (!schemas.isEmpty()) {
                for (Schema s : schemas) {
                    byte[] inverse = Helper.invertBits(s.writeable().getBytes());
                    outputStream.write(inverse);
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
    public String toString() {
        String output = "\n";
        if (!schemas.isEmpty()) {
            output += "Tables:";
            for (Schema schema : schemas) {
                output += "\n\n" + schema.toString();
            }
        }
        else {
            output += "No tables to display";
        }
        return output;
    }

    /**
     * getter method for the list of schemas
     *
     * @return the arraylist of schemas.
     */
    public ArrayList<Schema> getSchemas() {
        return schemas;
    }

    /**
     * Searches through the collection of schemas
     * and searches for one with the given name.
     * This method is case-sensitive.
     *
     * @return the schema, or null if not found.
     */
    public Schema getSchema(String name) {
        for (Schema schema: schemas) {
            if (schema.getName().equals(name)) {
                return schema;
            }
        }
        return null;
    }
}
