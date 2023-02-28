package src.StorageManager;

import src.Catalog.Schema;

import java.util.Map;

/**
 * The class for the representation of the page. This class
 * helps to parse through a page and perform different operations.
 *
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Page {
    Schema schema;
    byte[] data;
    int pageSize;

    /**
     * Constructor for records when we have a byte array.
     *
     * @param schema the schema the record uses.
     * @param pageSize the size of the page.
     * @param data the byte array that composes the record.
     */
    public Page (Schema schema, int pageSize, byte[] data) {
        this.schema = schema;
        this.data = data;
        this.pageSize = pageSize;
    }

    /**
     * Constructor for a new empty page.
     *
     * @param schema the schema the record uses.
     * @param pageSize the size of the page.
     * @param nextPage the next page to point to.
     */
    public Page (Schema schema, int pageSize, int nextPage) {
        this.schema = schema;
        this.pageSize = pageSize;
        this.data = new byte[pageSize];

        //put in the basic structure of the page
        //put zero as the number of records
        for (int byteIndex = 0; byteIndex < Integer.SIZE; byteIndex++) {
            data[byteIndex] = 0;
        }
        //put the pointer to the next page at the end of page
        for (int byteIndex = data.length - Integer.SIZE - 1; byteIndex < data.length; byteIndex++) {
            data[byteIndex] = 0;
        }

        //put the pointer to the free space
        int freeSpace = data.length - Integer.SIZE - 2;
        for (int byteIndex = Integer.SIZE ; byteIndex < Integer.SIZE*2; byteIndex++) {
            data[byteIndex] = (byte) (freeSpace >>> ((Integer.SIZE/8-1)*8 - (8 * byteIndex)));
        }
    }

    public Map<String, Object> getRecord(Object primaryKeyValue) {
        //TODO search records for primaryKey and return record.
        return null;
    }

    public boolean belongs(Object primaryKeyValue) {
        //TODO return true of key belongs in page, false otherwise
        return false;
    }

    public int addRecord(BufferManager bufferManager, Map<String, Object> attributes) {
        //TODO add a record, write to buffer, and if too full call split (call defragment first)
        return -1;
    }

    public int split(BufferManager bufferManager, Map<String, Object> attributes) {
        //TODO create a new page and add it to buffer.
        return -1;
    }

    public byte[] removeRecord(Object primaryKeyValue) {
        //TODO unassign pointer and return bytes.
        return null;
    }

    public void updateRecord(Object primaryKeyValue, Map<String, Object> attributes) {
        //TODO update the record with new values.
    }

    public byte[] getPage() {
        defragment();
        return data;
    }

    private void defragment() {
        //TODO if inneficient reorganise the page to be more space effecient.
    }
}
