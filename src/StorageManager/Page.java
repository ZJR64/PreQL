package src.StorageManager;

import src.Catalog.*;

import java.nio.ByteBuffer;
import java.util.*;

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
    private int pageSize;
    private int primaryKeyIndex;
    private int byteSize;
    private int intSize;

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
        this.primaryKeyIndex = findPrimaryKeyIndex();
        byteSize = Byte.SIZE;
        intSize = Integer.SIZE/Byte.SIZE;
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
        this.primaryKeyIndex = findPrimaryKeyIndex();
        intSize = Integer.SIZE/Byte.SIZE;

        //put in the basic structure of the page
        //put zero as the number of records
        for (int byteIndex = 0; byteIndex < intSize; byteIndex++) {
            data[byteIndex] = 0;
        }
        //put the pointer to the next page at the end of page
        for (int byteIndex = data.length - intSize - 1; byteIndex < data.length; byteIndex++) {
            data[byteIndex] = (byte) (nextPage >>> ((intSize-1)*8 - (8 * byteIndex)));
        }
        //put the pointer to the free space
        int freeSpace = data.length - intSize - 2;
        for (int byteIndex = intSize ; byteIndex < intSize*2; byteIndex++) {
            data[byteIndex] = (byte) (freeSpace >>> ((intSize/8-1)*8 - (8 * byteIndex)));
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
       Attribute key = schema.getKey();
       if (key.getType().equalsIgnoreCase("string")) {

       }
        if (key.getType().equalsIgnoreCase("integer")) {

        }
        if (key.getType().equalsIgnoreCase("double")) {

        }
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
        for (int byteIndex = data.length - intSize - 1; byteIndex < data.length; byteIndex++) {
            pagePointer = (pagePointer << byteSize) + (data[byteIndex] & 0xFF);
        }
        Page newPage = new Page(schema, pageSize, pagePointer);

        //TODO add records to the page

        //add page to buffer
        pagePointer = bufferManager.addPage(schema.getName(), newPage.getPage());
        //put the pointer to the next page at the end of page
        for (int byteIndex = data.length - intSize - 1; byteIndex < data.length; byteIndex++) {
            data[byteIndex] = (byte) (pagePointer >>> ((intSize-1)*byteSize - (byteSize * byteIndex)));
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
        for (int byteIndex = data.length - intSize - 1; byteIndex < data.length; byteIndex++) {
            pagePointer = (pagePointer << byteSize) + (data[byteIndex] & 0xFF);
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
    public void defragment() {
        //get the pointer to free space
        int freeSpace = 0;
        for (int byteIndex = intSize; byteIndex < intSize*2; byteIndex++) {
            freeSpace = (freeSpace << byteSize) + (data[byteIndex] & 0xFF);
        }

        //TODO if inneficient reorganise the page to be more space effecient.
    }

    /**
     * Finds the position for the pointer to the primary key in each record.
     *
     * @return the index of the primary key pointer.
     */
    private int findPrimaryKeyIndex() {
        int index = schema.getAttributes().indexOf(schema.getKey());
        index = index * intSize * 2; //account for integer size and the 2 ints
        return index;
    }

    /**
     * Gets the attributes of the record starting at the given index.
     *
     * @param index the index of the record to be read at.
     * @return map of attribute names and their values.
     */
    private Map<String, Object> getRecord(int index) {
        Map<String, Object> record = new HashMap<String, Object>();

        //find null bitmap and get it.
        int bitMapLocation = index + (intSize*2)*schema.getAttributes().size();
        int bitMapSize = (int) schema.getAttributes().size() / byteSize;
        byte[] bitMapBytes = new byte[bitMapSize];
        for (int byteIndex = 0; byteIndex < bitMapSize; byteIndex++) {
            bitMapBytes[byteIndex] = data[byteIndex + bitMapLocation];
        }

        //convert to BitSet
        BitSet nullBitMap = BitSet.valueOf(bitMapBytes);

        //parse through values
        int attributeIndex = 0;
        for (Attribute currentAttribute : schema.getAttributes()) {

            //check if null
            if(nullBitMap.get(attributeIndex)) {
                record.put(currentAttribute.getName(), null);
                continue;
            }

            int attributeLocation = 0;
            int attributeSize = 0;

            //get attribute location.
            int trueIndex = index + attributeIndex*intSize;
            for (int byteIndex = trueIndex; byteIndex < trueIndex + intSize; byteIndex++) {
                attributeLocation = (attributeLocation << byteSize) + (data[byteIndex] & 0xFF);
            }

            //get attribute size.
            trueIndex = index + attributeIndex*intSize + intSize;
            for (int byteIndex = trueIndex; byteIndex < trueIndex + intSize; byteIndex++) {
                attributeSize = (attributeSize << byteSize) + (data[byteIndex] & 0xFF);
            }

            //get attribute value
            byte[] attributeBytes = new byte[attributeSize];
            for (int byteIndex = 0; byteIndex < attributeSize; byteIndex++) {
                attributeBytes[byteIndex] = data[byteIndex+attributeLocation];
            }

            //put into map
            if (currentAttribute.getType().contains("char")) {
                record.put(currentAttribute.getName(), new String(attributeBytes));
            }
            else if (currentAttribute.getType().equalsIgnoreCase("integer")) {
                record.put(currentAttribute.getName(), ByteBuffer.wrap(attributeBytes).getInt());
            }
            else if (currentAttribute.getType().equalsIgnoreCase("double")) {
                record.put(currentAttribute.getName(),ByteBuffer.wrap(attributeBytes).getDouble());
            }
            else if (currentAttribute.getType().equalsIgnoreCase("boolean")) {
                int value = attributeBytes[0] & 0xFF;
                if (value == 0) {
                    record.put(currentAttribute.getName(), false);
                }
                else {
                    record.put(currentAttribute.getName(), true);
                }
            }

            //increment index
            attributeIndex++;
        }
        return record;
    }

    /**
     * turns a map of attributes consisting of names and values into a byte array.
     *
     * @param record the map of name and value that will be converted to bytes.
     * @return a byte array.
     */
    private void writeBytes(Map<String, Object> record, int arraySize) {

    }

    /**
     * finds the size of the byte array required for a record.
     *
     * @param record the map of name and value.
     * @return the potential size of a byte array.
     */
    private int getArraySize(Map<String, Object> record) {
        // calculate the size of the byte array
        int arraySize = intSize * 2 * schema.getAttributes().size();

        //go through each attribute
        for (Attribute attribute : schema.getAttributes()) {
            Object value = record.get(attribute.getName());

            //parse types
            if (value != null) {
                if (attribute.getType().contains("char")) {
                    //get length of string
                    arraySize += ((String) value).getBytes().length;
                } else if (attribute.getType().equalsIgnoreCase("integer")) {
                    //integer
                    arraySize += intSize;
                }
                else if (attribute.getType().equalsIgnoreCase("boolean")) {
                    //boolean is 1 byte
                    arraySize += 1;
                }
                else {
                    //must be double
                    arraySize += Double.SIZE/byteSize;
                }
            }
        }
        return arraySize;
    }
}
