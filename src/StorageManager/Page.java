package src.StorageManager;

import src.Catalog.Schema;

import java.util.Map;

/**
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Page {
    Map<String, Object> records;

    /**
     * Constructor for records when we have a byte array.
     *
     * @param schema the schema the record uses.
     * @param data the byte array that composes the record.
     */
    public Page (Schema schema, byte[] data) {
        createRecords(schema, data);
    }

    public Page(Map<String, Object> records) {
        this.records = records;
    }

    private void createRecords(Schema schema, byte[] data){

    }
}
