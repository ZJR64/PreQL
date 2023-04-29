package src.StorageManager;

import src.Catalog.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLOutput;
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

        //add to list
        addToList(newRecord);

        //record added successfully
        return true;
    }

    /**
     * Attempts to add the record to the page via a Record object with an index.
     *
     * @param newRecord the record being added.
     * @param index the location to add the index to.
     * @return true if operation completed, false if page needs to be split.
     */
    public boolean addRecordWithIndex(Record newRecord, int index) {
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

        //add to list
        recordList.add(index, newRecord);

        //update schema
        schema.addRecord();

        //update index values
        schema.getIndex().updateIndex(recordList, pageNum);

        //record added successfully
        return true;
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
        int newPageNum = bufferManager.addPage(schema.getFileName(), schema.getOpenPages());

        //create new page
        Page newPage = new Page(newPageNum, schema, pageSize, this.pageNum);

        //add new record so it can be split with others
        addToList(record);

        //add half the records to new page and then remove them
        int cutoffPoint = recordList.size() / 2;
        for (int i = cutoffPoint; i < recordList.size(); i++) {
            Record currentRecord = recordList.get(i);
            newPage.addRecord(currentRecord);
            //update schema
            schema.subRecord();
        }
        recordList.subList(cutoffPoint, recordList.size()).clear();

        //write new page to buffer
        bufferManager.writePage(schema.getFileName() , newPageNum, newPage.getBytes());

        // update the indexs of the records on the new page
        schema.getIndex().updateIndex(newPage.recordList, newPageNum);

        //return number of page
        return newPageNum;
    }

    /**
     * Splits the page, sending back the number of the page that was created.
     *
     * @param bufferManager the buffer manager.
     * @param record the record object being added.
     * @param index the index to add the record at
     * @return the number of the newly created page.
     */
    public int splitWithIndex(BufferManager bufferManager, Record record, int index) {
        //add page to buffer
        int newPageNum = bufferManager.addPage(schema.getFileName(), schema.getOpenPages());

        //create new page
        Page newPage = new Page(newPageNum, schema, pageSize, this.pageNum);

        //add to list
        recordList.add(index, record);

        //update schema
        schema.addRecord();

        //add half the records to new page and then remove them
        int cutoffPoint = recordList.size() / 2;
        for (int i = cutoffPoint; i < recordList.size(); i++) {
            Record currentRecord = recordList.get(i);
            schema.getIndex().removeFromIndex(currentRecord.getPrimaryKey());
            newPage.addRecord(currentRecord);
            //update schema
            schema.subRecord();
        }
        recordList.subList(cutoffPoint, recordList.size()).clear();

        //write new page to buffer
        bufferManager.writePage(schema.getFileName() , newPageNum, newPage.getBytes());

        // update the indexs of the records on the new page
        schema.getIndex().updateIndex(recordList, newPageNum);

        //return number of page
        return newPageNum;
    }


    /**
     * Removes a record from the table by primary key.
     *
     * @param primaryKeyValue the value of the primary key.
     */
    public void removeRecord(Object primaryKeyValue) {
        int removeIndex = 0;
        boolean found = false;
        for (int index = 0; index < recordList.size(); index++) {
            if(recordList.get(index).equals(primaryKeyValue)) {
                removeIndex = index;
                found = true;
            }
        }

        if (!found) {
            return;
        }

        recordList.remove(removeIndex);
        //increment schema
        schema.subRecord();

        //check if there are no more records
        if (recordList.isEmpty()) {
            //delete page
            schema.subPage(this.pageNum);
        }
    }

    /**
     * Removes a record from the table by primary key.
     *
     * @param index the index of the record to remove.
     */
    public void removeRecordWithIndex(int index) {
        //remove from list
        recordList.remove(index);

        //update the schema
        schema.subRecord();

        //update the index of all values
        schema.getIndex().updateIndex(recordList, pageNum);

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
    public void updateRecord(Record newRecord) {
        for (Record record : recordList) {
            if (record.equals(newRecord.getPrimaryKey())) {
                record.setAttributes(newRecord.getAttributes());
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

    private void addToList(Record newRecord) {
        //find where record belongs
        boolean inserted = false;
        for (int recordIndex = 0; recordIndex < recordList.size(); recordIndex++) {
            Record record = recordList.get(recordIndex);

            //check if greater than
            if(record.greaterThan(newRecord.getPrimaryKey())) {
                //add new record to arraylist
                recordList.add(recordIndex, newRecord);
                inserted = true;
                break;
            }
        }

        //insert at end if greatest value
        if (!inserted) {
            recordList.add(newRecord);
        }

        //insert if first record
        if (recordList.isEmpty()) {
            recordList.add(newRecord);
        }

        //increment schema
        schema.addRecord();
    }
}
