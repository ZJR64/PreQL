package src.StorageManager;

import src.Catalog.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.Predicate;

/**
 * The class for the representation of the page. This class
 * helps to parse through a page and perform different operations.
 *
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Page {
    private Schema schema;
    private ArrayList<Record> recordList;
    private int pageSize;
    private int pageNum;

    /**
     * Constructor for records when we have a byte array.
     *
     * @param schema the schema the record uses.
     * @param pageSize the size of the page.
     * @param data the byte array that composes the record.
     */
    public Page (int pageNum, Schema schema, int pageSize, byte[] data) {
        this.pageNum = pageNum;
        this.schema = schema;
        this.recordList = makeRecords(data);
        this.pageSize = pageSize;
    }

    /**
     * Constructor for a new empty page.
     *
     * @param schema the schema the record uses.
     * @param pageSize the size of the page.
     * @param pageNum The page number.
     */
    public Page (int pageNum, Schema schema, int pageSize, int beforePage) {
        this.pageNum = pageNum;
        this.schema = schema;
        this.pageSize = pageSize;
        this.recordList = new ArrayList<Record>();
        schema.addPage(beforePage, this.pageNum);
    }

    /**
     * Get all records from the page.
     *
     * @return the records represented by a dictionary full of attributes in a list.
     */
    public ArrayList<Record> getRecords() {
        return this.recordList;
    }

    /**
     * Gets a specific record, based on the primaryKey.
     *
     * @param primaryKeyValue the value of the primary key.
     * @return the record, or null if not found.
     */
    public Record getRecord(Object primaryKeyValue) {
        for (Record record : recordList) {
            System.out.println(record.getPrimaryKey() + ":" + primaryKeyValue);
            System.out.println(record.equals(primaryKeyValue));
            if (record.equals(primaryKeyValue)) {
                return record;
            }
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
        //for first record added to table
        if (recordList.isEmpty()) {
            return true;
        }

        //if last page then it definitely belongs
        ArrayList<Integer> order = schema.getPageOrder();
        if (order.get(order.size() - 1) == this.pageNum) {
            return true;
        }

        //search through the records to find a matching record
        for (Record record : recordList) {
            //check if equal
            if (record.equals(primaryKeyValue)) {
                return true;
            }
            //check if greater than
            if(record.greaterThan(primaryKeyValue)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to add the record to the page via an attribute map.
     *
     * @param attributes the attributes of the record that is being added.
     * @return true if operation completed, false if page needs to be split.
     */
    public boolean addRecord(Map<String, Object> attributes) {
        //get size already used
        int usedSize = 0;
        for (Record record : recordList) {
            //add an Integer for the size indicator
            usedSize += Integer.SIZE/Byte.SIZE;
            usedSize += record.getSize();
        }

        //create Record
        Record newRecord = new Record(this.schema, attributes);

        //now check to see if length of new record would exceed free space
        if (usedSize + newRecord.getSize() > pageSize) {
            //need to split page
            return false;
        }

        //find where record belongs
        for (int recordIndex = 0; recordIndex < recordList.size(); recordIndex++) {
            Record record = recordList.get(recordIndex);

            //check if greater than
            if(record.greaterThan(newRecord.getPrimaryKey())) {
                //add new record to arraylist
                recordList.add(recordList.indexOf(record), newRecord);
                break;
            }

            //check if last record
            if (recordList.indexOf(record) == recordList.size() - 1) {
                recordList.add(newRecord);
            }
        }

        //insert if first record
        if (recordList.isEmpty()) {
            recordList.add(newRecord);
        }

        //increment schema
        schema.addRecord();

        //record added successfully
        return true;
    }

    /**
     * Attempts to add the record to the page via a Record object.
     *
     * @param newRecord the record being added.
     * @return true if operation completed, false if page needs to be split.
     */
    public boolean addRecord(Record newRecord) {
        //get size already used
        int usedSize = 0;
        for (Record record : recordList) {
            //add an Integer for the size indicator
            usedSize += Integer.SIZE/Byte.SIZE;
            usedSize += record.getSize();
        }

        //now check to see if length of new record would exceed free space
        if (usedSize + newRecord.getSize() + Integer.SIZE/Byte.SIZE>  pageSize) {
            //need to split page
            return false;
        }

        //find where record belongs
        for (int recordIndex = 0; recordIndex < recordList.size(); recordIndex++) {
            Record record = recordList.get(recordIndex);
            Object currentKey = record.getPrimaryKey();

            //check if greater than
            if(currentKey instanceof Comparable && ((Comparable) currentKey).compareTo(newRecord.getPrimaryKey()) > 0) {
                //add new record to arraylist
                recordList.add(recordList.indexOf(record), newRecord);
                break;
            }

            //check if last record
            if (recordList.indexOf(record) == recordList.size() - 1) {
                recordList.add(newRecord);
            }
        }

        //insert if first record
        if (recordList.isEmpty()) {
            recordList.add(newRecord);
        }

        //increment schema
        schema.addRecord();

        //record added successfully
        return true;
    }

    /**
     * Splits the page, sending back the number of the page that was created.
     *
     * @param bufferManager the buffer manager.
     * @param attributes the attributes of the record that is being added.
     * @return the number of the newly created page.
     */
    public int split(BufferManager bufferManager, Map<String, Object> attributes) {
        //add page to buffer
        int newPageNum = bufferManager.addPage(schema.getName(), schema.getOpenPages());

        //create new page
        Page newPage = new Page(newPageNum, schema, pageSize, this.pageNum);

        //add half the records to new page and then remove them
        int cutoffPoint = recordList.size()/2;
        while (cutoffPoint < recordList.size()) {
            Record currentRecord = recordList.get(cutoffPoint);
            newPage.addRecord(currentRecord.getAttributes());
            recordList.remove(currentRecord);
        }

        //write new page to buffer
        bufferManager.writePage(schema.getFileName() , newPageNum, newPage.getBytes());

        //add new record
        addRecord(attributes);

        //return number of page
        return newPageNum;
    }

    /**
     * Splits the page, sending back the number of the page that was created.
     *
     * @param bufferManager the buffer manager.
     * @param record the record object being added.
     * @return the number of the newly created page.
     */
    public int split(BufferManager bufferManager, Record record) {
        //add page to buffer
        int newPageNum = bufferManager.addPage(schema.getName(), schema.getOpenPages());

        //create new page
        Page newPage = new Page(newPageNum, schema, pageSize, this.pageNum);

        //add half the records to new page and then remove them
        int cutoffPoint = recordList.size()/2;
        while (cutoffPoint < recordList.size()) {
            Record currentRecord = recordList.get(cutoffPoint);
            newPage.addRecord(currentRecord.getAttributes());
            recordList.remove(currentRecord);
        }

        //write new page to buffer
        bufferManager.writePage(schema.getFileName() , newPageNum, newPage.getBytes());

        //add page to schema
        schema.addPage(this.pageNum, newPageNum);

        //add new record
        addRecord(record);

        //return number of page
        return newPageNum;
    }

    /**
     * Removes a record from the table by primary key.
     *
     * @param primaryKeyValue the value of the primary key.
     */
    public void removeRecord(Object primaryKeyValue) {
        for (Record record : recordList) {
            if(record.equals(primaryKeyValue)) {
                recordList.remove(record);
            }
        }
        //increment schema
        schema.subRecord();

        //check if there are no more records
        if (recordList.isEmpty()) {
            //delete page
            schema.subPage(this.pageNum);
        }
    }

    /**
     * Updates a record from the table with new values.
     *
     * @param attributes the attributes to overwrite with.
     */
    public void updateRecord(Map<String, Object> attributes) {
        Object primaryKey = attributes.get(schema.getKey().getName());
        for (Record record : recordList) {
            if (record.equals(primaryKey)) {
                record.setAttributes(attributes);
            }
        }
    }

    /**
     * Makes the page into a byteArray.
     *
     * @return the byte array of the page.
     */
    public byte[] getBytes() {
        //setup

        ByteBuffer buffer = ByteBuffer.wrap(new byte[this.pageSize]).order(ByteOrder.BIG_ENDIAN);

        //store number of records
        int numRecords = recordList.size();
        buffer.putInt(numRecords);

        //store records
        for (Record record : recordList) {
            byte[] recordBytes = record.getBytes();
            buffer.put(recordBytes);
        }

        //return byte array
        byte[] array = buffer.array();
        buffer.clear();
        return array;
    }

    /**
     * Add an attribute to each record with a default value.
     *
     * @return the arraylist of records.
     */
    public ArrayList<Record> deletePage() {
        schema.subPage(this.pageNum);
        return recordList;
    }

    /**
     * Creates the records for the page.
     *
     * @param data the byte array of the page.
     * @return the arraylist of records.
     */
    private ArrayList<Record> makeRecords(byte[] data) {
        //setup
        ArrayList<Record> records = new ArrayList<Record>();

        //create ByteManager
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

        //get numRecords
        int numRecords = buffer.getInt();

        for (int recordInt = 0; recordInt < numRecords; recordInt++) {
            Record newRecord = new Record(schema, buffer);
            records.add(newRecord);
        }

        //return records
        buffer.clear();
        return records;
    }
}
