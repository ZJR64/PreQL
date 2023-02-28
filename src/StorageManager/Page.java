package src.StorageManager;

import src.Catalog.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The class for the representation of the page. This class
 * helps to parse through a page and perform different operations.
 *
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Page {
    private Schema schema;
    private byte[] data;
    private Map<Integer, Integer> waste;
    private int pageSize;

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
        this.waste = new HashMap<Integer, Integer>();
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
            data[byteIndex] = (byte) (nextPage >>> ((Integer.SIZE-1)*8 - (8 * byteIndex)));
        }

        //put the pointer to the free space
        int freeSpace = data.length - Integer.SIZE - 2;
        for (int byteIndex = Integer.SIZE ; byteIndex < Integer.SIZE*2; byteIndex++) {
            data[byteIndex] = (byte) (freeSpace >>> ((Integer.SIZE/8-1)*8 - (8 * byteIndex)));
        }
    }

    /**
     * Get all records from the page.
     *
     * @return the records represented by a dictionary full of attributes in a list.
     */
    public Map[] getRecords() {
        //TODO go through records and return in order.
        return null;
    }

    /**
     * Gets a specificrecord, based on the primaryKey.
     *
     * @param primaryKeyValue the value of the primary key.
     * @return the record represented by a dictionary full of attributes.
     */
    public Map<String, Object> getRecord(Object primaryKeyValue) {
        //TODO search records for primaryKey and return record.
        return null;
    }

    /**
     * Checks to see if this record belongs in the page.
     *
     * @param primaryKeyValue the value of the primary key.
     * @return true primary key exists, or could potentially exist in page, false otherwise.
     */
    public boolean belongs(Object primaryKeyValue) {
        //TODO return true of key belongs in page, false otherwise
        return false;
    }

    /**
     * Checks to see if this record belongs in the page.
     *
     * @param attributes the attributes of the record that is being added.
     * @return true if operation completed, false if page needs to be split.
     */
    public boolean addRecord(Map<String, Object> attributes) {
        //TODO add a record, write to buffer, and if too full return false (defragment first)
        return false;
    }

    /**
     * Splits the page, sending back a page object of the new page.
     * to the page
     * @param bufferManager the buffer manager, to add
     * @param attributes the attributes of the record that is being added.
     * @return the number of the newly created page.
     */
    public int split(BufferManager bufferManager, Map<String, Object> attributes) {
        //extract nextPage
        int pagePointer = 0;
        for (int byteIndex = data.length - Integer.SIZE - 1; byteIndex < data.length; byteIndex++) {
            pagePointer = (pagePointer << 8) + (data[byteIndex] & 0xFF);
        }
        Page newPage = new Page(schema, pageSize, pagePointer);

        //TODO add records to the page

        //add page to buffer
        pagePointer = bufferManager.addPage(schema.getName(), newPage.getPage());
        //put the pointer to the next page at the end of page
        for (int byteIndex = data.length - Integer.SIZE - 1; byteIndex < data.length; byteIndex++) {
            data[byteIndex] = (byte) (pagePointer >>> ((Integer.SIZE-1)*8 - (8 * byteIndex)));
        }
        return pagePointer;
    }

    /**
     * Removes a record from the table.
     *
     * @param primaryKeyValue the value of the primary key.
     * @return the bytes of the record being removed.
     */
    public byte[] removeRecord(Object primaryKeyValue) {
        //TODO unassign pointer, add to waste, and return bytes.
        return null;
    }

    /**
     * Updates a record from the table with new values.
     *
     * @param attributes the attributes to overwrite with.
     */
    public void updateRecord(Map<String, Object> attributes) {
        //TODO update the record with new values.
    }

    /**
     * Returns the page number of the page this page points to.
     * Quite the tounge twister.
     *
     * @return the number of the next page.
     */
    public int getNextPage() {
        //extract nextPage
        int pagePointer = 0;
        for (int byteIndex = data.length - Integer.SIZE - 1; byteIndex < data.length; byteIndex++) {
            pagePointer = (pagePointer << 8) + (data[byteIndex] & 0xFF);
        }
        return pagePointer;
    }

    /**
     * Getter method for the page's byte array. Calls defragment first to reduce fragmentation.
     *
     * @return the byte array of the page.
     */
    public byte[] getPage() {
        defragment();
        return data;
    }

    /**
     * Looks at the wasted space in the page and shifts over all values, could be potentially
     * high cost, so perhaps some tricks could be used.
     */
    private void defragment() {
        //return if no waste is reported.
        if (waste.isEmpty()) {
            return;
        }
        //TODO if inneficient reorganise the page to be more space effecient.
    }
}
