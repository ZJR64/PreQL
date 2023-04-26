package src.Catalog;

import src.Helper;
import src.StorageManager.BufferManager;

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
    private ArrayList<Schema> schemas;
    private boolean indexing;
    private BufferManager bufferManager;


    /**
     * Constructor for the Catalog object
     *
     * @param catPath the file path to the catalog.
     * @param pageSize the size of each page of the database.
     */
    public Catalog(String catPath, int pageSize, boolean indexing, BufferManager bufferManager) {
        this.schemas = new ArrayList<Schema>();
        this.catPath = catPath;
        this.indexing = indexing;
        if (indexing) {
            this.bufferManager = bufferManager;
        }
        if (Helper.checkFile(catPath)) {
            //make the catalog read from file
            byte[] byteArray;
            try {
                FileInputStream inputStream = new FileInputStream(catPath);
                byteArray = inputStream.readAllBytes();
                inputStream.close();
            } catch (Exception e) {
                System.err.println("Error: catalog file could not be read");
                return;
            }
            //put into ByteBuffer
            ByteBuffer buffer = ByteBuffer.wrap(Helper.invertBits(byteArray));

            //get numSchemas
            int numSchemas = buffer.getInt();

            //get schemas
            while (schemas.size() < numSchemas) {
                schemas.add(new Schema(buffer));
            }
        }
    }

    /**
     * Method that safely stores the Catalog when the database is shut down.
     */
    public void shutDown(){
        //shut down
        bufferManager.shutDown();

        //make bytes
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getCatalogByteSize()]);

        //set numSchemas
        buffer.putInt(schemas.size());

        //set each schema
        for (Schema schema : schemas) {
            byte[] schemaBytes = schema.toBytes();
            buffer.put(schemaBytes);
        }

        //get bytes to write
        byte[] output = Helper.invertBits(buffer.array());

        try {
            FileOutputStream outputStream = new FileOutputStream(catPath);

            //write bytes
            outputStream.write(output);

            //close
            outputStream.close();

        } catch (Exception e) {
            System.err.println("Error: catalog file could not be written to");
            System.err.println(e);
            return;
        }
    }

    public BufferManager getBufferManager() {
        return bufferManager;
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

    /**
     * Adds a Schema to the catalog
     *
     * @param schema the Schema object to add to catalog
     */
    public void addSchema(Schema schema) {
        schemas.add(schema);
    }

    /**
     * Removes Schema from catalog
     *
     * @param schema the Schema object to remove from catalog
     */
    public void removeSchema(Schema schema) {
        schemas.remove(schema);
    }

    /**
     * get the size of a byte array for the schema
     *
     * @return the size of a byte array
     */
    private int getCatalogByteSize() {
        int arraySize = 0;

        //add Integer for the number of schemas
        arraySize += Integer.SIZE/Byte.SIZE;

        //add bytes of each schema
        for (Schema schema : schemas) {
            arraySize += schema.getSchemaByteSize();
        }

        return arraySize;
    }
}
