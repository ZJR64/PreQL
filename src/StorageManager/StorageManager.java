package src.StorageManager;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import src.Catalog.*;
import src.Commands.Node;
import src.Commands.NodeType;
import src.Commands.WhereClause;



/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    public BufferManager bm;
    private Catalog c;

    public StorageManager(BufferManager bm, Catalog c){
        this.bm = bm;
        this.c = c;
    }

    /**
     * Inserts into a table a number of records.
     *
     * @param tableName table being inserted into
     * @param tuples the records to be inserted into the table.
     * @return A string reporting the success/failure of the command.
     */
    public String insert(String tableName, ArrayList<ArrayList<String>> tuples) {
        Schema table = c.getSchema(tableName);
        Map<String, Object> attributes;
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }
        String fileName = table.getFileName();

        for(ArrayList<String> tuple : tuples){  // for tuple in tuples, for loop will create tuples into records.
            attributes = StorageManagerHelper.checkAttributes(table, tuple, bm);
            if(attributes != null){ // we're good, the tuple is valid and we can make the record.
                //check if first page
                if(table.getPages() == 0){
                    bm.addPage(fileName, table.getOpenPages());
                    Page pg = new Page(0, table, bm.getPageSize(), 0);
                    bm.writePage(fileName, 0, pg.getBytes());
                }
                ArrayList<Integer> pgOrder = table.getPageOrder();
                //create record
                Record rec = new Record(table, attributes);
                //iterate through pages
                for (Integer i : pgOrder) {

                    //get page from buffer
                    Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                    //check if belongs
                    if (pg.belongs(rec.getPrimaryKey())) {
                        //add to page if belongs
                        if (!pg.addRecord(rec)) {
                            //split page if false
                            pg.split(bm, rec);
                        }
                        //write to buffer
                        bm.writePage(fileName, i, pg.getBytes());

                        //break because record has been added
                        break;
                    }
                }
            }
            else{
                return "\nERROR";
            }
        }
        return "SUCCESS";
    }


    /**
     * Iterates through all the pages associated with a table, getting all the
     * records for that table and returns it.
     *
     * @param tableName the table whose records are being gotten.
     * @return An ArrayList of all the records associated with the tableName.
     */
    public ArrayList<Record> getAllRecords(String tableName){
        Schema table = c.getSchema(tableName);
        ArrayList<Integer> pageList = table.getPageOrder();
        ArrayList<Record> finalRecords = new ArrayList<>();
        for(int pgNum : pageList){
            byte[] bytes =  bm.getPage(table.getFileName(), pgNum);
            Page pg = new Page(pgNum, table, bm.getPageSize(), bytes);
            finalRecords.addAll(pg.getRecords());
        }
        return finalRecords;
    }

    /**
     * Gets all records from the given table.
     *
     * @param tableNames The list of tables being selected from.
     * @param where The root node of the where tree.
     * @param orderBy Null if no orderBy, the column names to display otherwise.
     * @param columns
     * @return A string reporting the success/failure of the command.
     */
    public String select(String[] tableNames, WhereClause where, String orderBy, String [] columns){
        for(String tableName : tableNames){
            Schema table = c.getSchema(tableName);
            if(table == null){
                return "No such table " + tableName.concat("\nERROR");  //returns an error if there is no table
            }
        }
        ArrayList<Record> recs;
        if(tableNames.length > 1) {
            recs = fromClause(tableNames);

        }
        else{
            recs = getAllRecords(tableNames[0]);
        }

        if(where != null){
            for (Record r : recs) {
                if (!whereClause(where.getRoot(), r)){
                    recs.remove(r);
                }
            }
        }
        if(orderBy != null){

        }





        // Make sure to unchange names of attributes!!!
        return "SUCCESS";
    }


    /**
     * Combines all tables into one mega table with renamed attributes.
     *
     * @param tableNames The tables to be cartesian Product'd.
     * @return the combined records ArrayList.
     */
    private ArrayList<Record> fromClause(String[] tableNames){
        int iter = 0;
        for(String tblNm : tableNames){
            Schema table = c.getSchema(tableNames[iter]);
            iter++;
            for (Attribute attr : table.getAttributes()) {
                attr.changeName(tblNm + "." + attr.getName());
            }
        }
        ArrayList<Record> combinedRecs = cartesianProduct(getAllRecords(tableNames[0]), getAllRecords(tableNames[1]));
        for(int i = 2; i < tableNames.length; i++){
            combinedRecs = cartesianProduct(combinedRecs, getAllRecords(tableNames[i]));
        }
        return combinedRecs;
    }

    private ArrayList<Record> cartesianProduct(ArrayList<Record> recs1, ArrayList<Record> recs2){
        ArrayList<Record> newRecs = new ArrayList<>();
        for (Record r1 : recs1) {
            for (Record r2 : recs2) {
                Map<String, Object> tempMap = new HashMap<>();
                for (String s : r1.getAttributes().keySet()){
                    tempMap.put(s, r1.getAttributes().get(s));
                }
                for (String s : r2.getAttributes().keySet()){
                    tempMap.put(s, r2.getAttributes().get(s));
                }
                newRecs.add(new Record( r1.getSchema(), tempMap));
            }
        }
        return newRecs;
    }

    /**
     * Can a tree be uneven?
     * What about x = y = z
     * What about x and/or y
     */
    /**
     * Goes through the whereClause tree.
     */
    public Boolean whereClause(Node root, Record rec){
        if (root.getLeft().getType() != NodeType.VALUE){
            whereClause(root.getLeft(), rec);
        }
        else {

        }
        if (root.getRight().getType() != NodeType.VALUE){
            whereClause(root.getRight(), rec);
        }

        return true;
    }


    /**
     * Takes in an arrayList of records to be reprinted, and copies them into a
     * new Record arrayList that has them inserted in the proper order so that
     * iterating through it will print out the records in correct order.
     *
     * @param columns The columns that are determining the order of the table.
     * @param records The arrayList of records to reorder.
     * @param tableNames only needed due to a record needing a schema specified
     *                  when created. What schema used doesn't matter.
     * @return an ArrayList of records that were inserted in the proper order.
     */
    private ArrayList<Record> orderBy(String columns, ArrayList<Record> records, String[] tableNames) {
        Schema table = c.getSchema(tableNames[0]);
        String[] cols = columns.split(",");

        for(int i = 1; i < records.size(); i++){
            Record temp = records.get(i);
            int j = i;
            while((j > 0) && (compareAttVal(records.get(j-1), temp, cols) == 1)){

            }
        }
        return records;
    }


    /**
     * Basically a comparator that checks the object type being compared, and
     * returns whether its greater than, equal to, or less than another object.
     * Additonally will compare by any number of attributes passed in.
     *
     * @param rec1 The first record who's being compared.
     * @param rec2 The second record who's being compared
     * @param cols The attributes that will be used to be compared.
     * @return 1 if rec1 > rec2, 0 if equal, -1 if rec1 < rec2.
     */
    private int compareAttVal(Record rec1, Record rec2, String [] cols){
        for(String col: cols){
            for(Map.Entry<String, Object> entry : rec1.getAttributes().entrySet()){
                String key = entry.getKey();
                if(key.equals(col)){
                    Object obj1 = rec1.getValue(key);
                    Object obj2 = rec2.getValue(key);
                    if(true){
                        return 1;
                    }
                    else{
                        return -1;
                    }

                }
            }
        }
        return 0;
    }

    /**
     * Deletes all records given to it from the desired table.
     *
     * @param records the arrayList of records to delete.
     * @param tableName The table the records are being gotten from.
     * @return A string reporting the success/failure of the command.
     */
    public String deleteRecords(ArrayList<Record> records, String tableName){
        Schema table = c.getSchema(tableName);
        ArrayList<Integer> pageList = table.getPageOrder();
        for (Integer pageNum : pageList){
            byte[] bytes =  bm.getPage(table.getFileName(), pageNum);
            Page page = new Page(pageNum, table, bm.getPageSize(), bytes);
            //go through record list
            for (Record record : records) {
                if (page.belongs(record.getKey())) {
                    //remove from table
                    page.removeRecord(record.getKey());
                    //remove from array
                    records.remove(record);
                }
            }

            //write to buffer
            bm.writePage(table.getFileName(), pageNum, page.getBytes());
        }

        //check if any records left
        if (records.size() > 0) {
            return records.size() + " records not deleted.\nERROR";  //returns an error if not all records deleted
        }

        return "SUCCESS";
    }

    /**
     * Updates all records to match the target value. Does several integrity checks for the database.
     *
     * @param records the arrayList of records to delete.
     * @param tableName The table the records are being gotten from.
     * @param target the name of the attribute to be changed.
     * @param value the value to set the target to.
     * @return A string reporting the success/failure of the command.
     */
    public String updateRecords(ArrayList<Record> records, String tableName, String target, Object value){
        //get attribute and schema
        Schema table = c.getSchema(tableName);
        Attribute targetAttribute = table.getAttribute(target);

        //check if value is right type
        if (targetAttribute.getType().contains("char")) {
            //varchar or char
            try {
                String valueString = (String) value;
            } catch(Exception e) {
                return value.toString() + " is not a string, but should be.\nERROR";  //returns an error
            }
        }
        else if (targetAttribute.getType().equalsIgnoreCase("integer")) {
            //integer
            try {
                int valueInteger = (int) value;
            } catch(Exception e) {
                return value.toString() + " is not an integer, but should be.\nERROR";  //returns an error
            }
        }
        else if (targetAttribute.getType().equalsIgnoreCase("boolean")) {
            //boolean
            try {
                boolean valueBoolean = (boolean) value;
            } catch(Exception e) {
                return value.toString() + " is not a boolean, but should be.\nERROR";  //returns an error
            }
        }
        else {
            //double
            try {
                double valueDouble = (double) value;
            } catch(Exception e) {
                return value.toString() + " is not a double, but should be.\nERROR";  //returns an error
            }
        }

        //check if value is unique
        //TODO how to handle unique and primaryKey
        for (String descriptor : targetAttribute.getDescriptors()) {
            if (descriptor.equals("unique")) {
                if (records.size() > 1) {
                    return records.size() + " is too many updates for a unique value.\nERROR";  //returns an error
                }
                if (!StorageManagerHelper.checkUniqueness(table, value, bm, target)) {
                    return target + " already exists in the table and is supposed to be unique.\nERROR";  //returns an error
                }
            }
        }

        ArrayList<Integer> pageList = table.getPageOrder();
        for (Integer pageNum : pageList){
            byte[] bytes =  bm.getPage(table.getFileName(), pageNum);
            Page page = new Page(pageNum, table, bm.getPageSize(), bytes);
            //go through record list
            for (Record record : records) {
                if (page.belongs(record.getKey())) {
                    //remove from table
                    page.updateRecord(record);
                    //remove from array
                    records.remove(record);
                }
            }
        }

        //check if any records left
        if (records.size() > 0) {
            return records.size() + " records not updated.\nERROR";  //returns an error if not all records updated
        }

        return "SUCCESS";
    }

    /**
     * Attempts to create a new table in the database.
     *
     * @param name The name of the table being created.
     * @param attributes The list of attributes the table will have.
     * @return A string reporting success or failure.
     */
    public String createTable(String name, ArrayList<Attribute> attributes){
        for (Schema i : c.getSchemas()) {
            if(i.getName().equals(name)){
                return "Table already exists" +
                        "\nERROR";
            }
        }

        Schema new_table = new Schema(name, attributes);
        c.addSchema(new_table);
        return "SUCCESS";
    }

    /**
     * Attempts to drop the table from the database.
     *
     * @param tableName name of the table.
     * @return A string reporting success or failure.
     */
    public String dropTable(String tableName){
        Schema schema = c.getSchema(tableName);
        if (schema == null){
            return "Table "+ tableName +" could not be found" + "\n ERROR";
        }
        bm.purge(schema.getFileName());
        c.removeSchema(schema);
        return "Success";
    }


    /**
     * Attempts to alter a table.
     *
     * @param tableName The name of the table being altered.
     * @param alterType Specifies whether a drop or add alteration is occurring.
     * @param attributeName The name of the attribute being added or dropped.
     * @param attributeType Type of the attribute being added, null if attribute is being dropped.
     * @param defaultValue What the table will fill in all new entries with, null if drop occurring.
     * @return A string reporting success or failure.
     */
    public String alterTable(String tableName, String alterType,
                             String attributeName, String attributeType,
                             String defaultValue) {
        Schema schema = c.getSchema(tableName);
        if(schema == null){
            return "Table " + tableName + " could not be found" + "\n ERROR";
        }

        if(alterType.equals("add")){   // add
            return StorageManagerHelper.alterAdd(schema, attributeName,
                    attributeType, defaultValue, bm);
        }
        else{ // drop
            return StorageManagerHelper.alterDrop(schema, attributeName, bm);

        }
    }
}
