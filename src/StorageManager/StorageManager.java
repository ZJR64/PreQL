package src.StorageManager;

import java.util.*;

import src.Catalog.*;
import src.Commands.Node;
import src.Commands.NodeType;
import src.Commands.WhereClause;
import src.Index.Index;


/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    public BufferManager bm;
    private Catalog c;
    private boolean indexing;

    public StorageManager(BufferManager bm, Catalog c, boolean indexing){
        this.bm = bm;
        this.c = c;
        this.indexing = indexing;
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
     * Finds the records from the table that comply with the where clause.
     *
     * @param tableName the table whose records are being gotten.
     * @param where the where statement to check the records against.
     * @return An ArrayList of all the records that meet the requirements.
     */
    public ArrayList<Record> getRecords(String tableName, WhereClause where) {
        Schema schema = c.getSchema(tableName);
        if(schema == null){
            return null;
        }
        ArrayList<Attribute> attributes = schema.getAttributes();

        //change attribute names
        for(Attribute attribute : attributes){
            attribute.changeName(tableName + "." + attribute.getName());
        }

        ArrayList<Record> records = getAllRecords(tableName);

        ArrayList<Record> chosenRecords = new ArrayList<Record>();
        if(where != null){
            for (Record record : records) {
                Boolean belongs = whereClause(where.getRoot(), record, attributes);
                if (belongs == null) {
                    return null;
                }
                if (belongs){
                    chosenRecords.add(record);
                }
            }
        }
        else{
            chosenRecords = records;
        }

        //change attribute names back
        for (Attribute attribute : attributes) {
            String[] temp = attribute.getName().split("\\.");
            attribute.changeName(temp[1]);
        }
        for (Record record : chosenRecords) {
            Map<String, Object> elements = record.getAttributes();
            Map<String, Object> newElements = new HashMap<String, Object>();
            for (String name : elements.keySet()) {
                String[] temp = name.split("\\.");
                newElements.put(temp[1], elements.get(name));
            }
            record.setAttributes(newElements);
        }

        return chosenRecords;
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
        ArrayList<String> prints = new ArrayList<>();
        if (columns[0].equals("*")){
            for (String s : tableNames) {
                if(c.getSchema(s) == null){
                    return "No such table " + s.concat("\nERROR");
                }
                for (Attribute a : c.getSchema(s).getAttributes()) {
                    prints.add(a.getName());
                }
            }
        }
        else {
            for (String s : columns) {
                prints.add(s);
            }
        }
        ArrayList<Attribute> allAttr = new ArrayList<>();
        for(String tableName : tableNames){
            Schema table = c.getSchema(tableName);
            if(table == null){
                return "No such table " + tableName.concat("\nERROR");  //returns an error if there is no table
            }
            for(Attribute attr : table.getAttributes()){
                allAttr.add(attr);
                attr.changeName(tableName + "." + attr.getName());
            }
        }
        ArrayList<Record> recs;
        if(tableNames.length > 1) {
            recs = fromClause(tableNames);

        }
        else{
            recs = getAllRecords(tableNames[0]);
        }
        ArrayList<Record> tempRecs = new ArrayList<>();

        if(where != null){
            for (Record r : recs) {
                Boolean temp = whereClause(where.getRoot(), r, allAttr);
                if (temp == null){
                    for(String tblNm : tableNames) {
                        Schema table = c.getSchema(tblNm);
                        for (Attribute attr : table.getAttributes()) {
                            String[] tempString = attr.getName().split("\\.");
                            attr.changeName(tempString[1]);
                        }
                    }
                    return "ERROR: Where clause failed";
                }
                if (temp){
                    tempRecs.add(r);
                }
            }
        }
        else{
            tempRecs = recs;
        }

        if(orderBy != null){
            Integer res = orderBy(orderBy, tempRecs, allAttr);
            if(res == null){
                return "\nERROR";
            }
        }


        String str = "";
        if (columns[0].equals("*")){
            for (Attribute a : allAttr) {
                str += String.format("| %20.20s |", a.getName());
            }
        }
        else {
            for (String s : columns) {
                if (s.contains(".")){
                    str += String.format("| %20.20s |", s);
                }
                else{
                    int count = 0;
                    for (Attribute a : allAttr) {
                        if (a.getName().split("\\.")[1].equals(s)){
                            count++;
                            str += String.format("| %20.20s |", a.getName());
                        }
                        if (count > 1){
                            return "ERROR: specify attribute names";
                        }
                    }
                }
            }
        }
        str += "\n";
        for (Record r : tempRecs) {
            if (columns[0].equals("*")){
                for (Attribute a : allAttr) {
                    str += String.format("| %20.20s |", r.getAttributes().get(a.getName()));
                }
            }
            else {
                for (String s : columns) {
                    if (s.contains(".")){
                        str += String.format("| %20.20s |", r.getAttributes().get(s));
                    }
                    else{
                        int count = 0;
                        for (Attribute a : allAttr) {
                            if (a.getName().split("\\.")[1].equals(s)){
                                count++;
                                str += String.format("| %20.20s |", r.getAttributes().get(a.getName()));
                            }
                            if (count > 1){
                                return "ERROR: specify attribute names";
                            }
                        }
                    }
                }
            }
            str += "\n";
        }
        System.out.println(str);

        for(String tblNm : tableNames) {
            Schema table = c.getSchema(tblNm);
            for (Attribute attr : table.getAttributes()) {
                String[] temp = attr.getName().split("\\.");
                attr.changeName(temp[1]);
            }
        }
        return "SUCCESS";
    }


    /**
     * Combines all tables into one mega table with renamed attributes.
     *
     * @param tableNames The tables to be cartesian Product'd.
     * @return the combined records ArrayList.
     */
    private ArrayList<Record> fromClause(String[] tableNames){
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
     * Can a tree be uneven? yes but leaves are always values
     * What about x = y = z no
     * What about x and/or y no
     * <, >, <=, >= is possible but not +, -, x, etc.
     */
    /**
     * Goes through the whereClause tree.
     */
    public Boolean whereClause(Node root, Record rec, ArrayList<Attribute> atts){
        if (root.getType() == NodeType.COMPARATOR){
            return StorageManagerHelper.compare(rec, root.getLeft().getValue(), root.getRight().getValue(), root.getValue(), atts);
        }
        else{
            Boolean left = whereClause(root.getLeft(), rec, atts);
            Boolean right = whereClause(root.getRight(), rec, atts);
            if (left == null || right == null){
                return null;
            }
            if (root.getValue().equals("and")){
                return left && right;
            }
            else if (root.getValue().equals("or")){
                return left || right;
            }
            else {
                System.out.println("ERROR: Tree parse error");
                return null;
            }
        }
    }


    /**
     * Takes in an arrayList of records to be reprinted, and copies them into a
     * new Record arrayList that has them inserted in the proper order so that
     * iterating through it will print out the records in correct order.
     *
     * @param columns The columns that are determining the order of the table.
     * @param records The arrayList of records to reorder.
     * @param attrs ArrayList of all attributes. Used to get types.
     */
    private Integer orderBy(String columns, ArrayList<Record> records,  ArrayList<Attribute> attrs) {
        String[] cols = columns.split(",");
        for(int i = 0; i < cols.length; i++){
            cols[i] = cols[i].strip();
        }
        int j;
        for(int i = 1; i < records.size(); i++){
            Record temp = records.get(i);
            j = i;
            Integer result = 0;
            while((j > 0) && (result = compareAttVal(records.get(j - 1), temp, cols, attrs)) != null && compareAttVal(records.get(j - 1), temp, cols, attrs) == 1){
                records.set(j, records.get(j-1));
                j--;
            }
            if(result == null){
                return null;
            }
            records.set(j, temp);
        }
        return 0;
    }


    /**
     * Basically a comparator that checks the object type being compared, and
     * returns whether it's greater than, equal to, or less than another object.
     * Additonally will compare by any number of attributes passed in.
     *
     * @param rec1 The first record who's being compared.
     * @param rec2 The second record who's being compared
     * @param cols The attributes that will be used to be compared.
     * @return 1 if rec1 > rec2, 0 if equal, -1 if rec1 < rec2.
     */
    private Integer compareAttVal(Record rec1, Record rec2, String [] cols, ArrayList<Attribute> attrs){
        for(String col: cols){
            for(Map.Entry<String, Object> entry : rec1.getAttributes().entrySet()){
                String key = entry.getKey();
                String[] splitKey = key.split("\\.");
                if(!(col.contains("."))) {  // if col doesn't contain a ., we need to ensure that the name of it is unique.
                    int howMany = 0;
                    for (Map.Entry<String, Object> entry2 : rec1.getAttributes().entrySet()) {
                        String key2 = entry2.getKey();
                        String[] splitKey2 = key2.split("\\.");
                        if(splitKey2[1].equals(col)){
                            howMany++;
                            if(howMany == 2){
                                System.out.println("DUPLICATE ATTRIBUTE NAMES! MUST SPECIFY tableName.Attribute");
                                return null;
                            }
                        }

                    }
                }

                if(key.equals(col) || splitKey[1].equals(col)){
                    Object obj1 = rec1.getValue(key);
                    Object obj2 = rec2.getValue(key);
                    for(Attribute attr : attrs){
                        if(key.equals(attr.getName())){
                            String objsType = attr.getType();
                            if(objsType.contains("char")){  //varchar or char
                                String val1 = (String) obj1;
                                String val2 = (String) obj2;
                                if(val1 == null ){
                                    return -1;
                                }
                                if(val2 == null){
                                    return 1;
                                }
                                int res = val1.compareTo(val2);
                                if(res == 0){
                                    continue;
                                }
                                else{
                                    return res;
                                }
                            }
                            else if(objsType.equalsIgnoreCase("integer")){
                                Integer val1 = (Integer) obj1;
                                Integer val2 = (Integer) obj2;
                                if(val1 == null ){
                                    return -1;
                                }
                                if(val2 == null){
                                    return 1;
                                }
                                int res = Integer.compare(val1, val2);
                                if(res == 0){
                                    continue;
                                }
                                else{
                                    return res;
                                }
                            }
                            else if(objsType.equalsIgnoreCase("boolean")){
                                Boolean val1 = (Boolean) obj1;
                                Boolean val2 = (Boolean) obj2;
                                if(val1 == null ){
                                    return -1;
                                }
                                if(val2 == null){
                                    return 1;
                                }
                                int res = Boolean.compare(val1, val2);
                                if(res == 0){
                                    continue;
                                }
                                else{
                                    return res;
                                }
                            }
                            else{ // double

                                Double val1 = (Double) obj1;
                                Double val2 = (Double) obj2;
                                if(val1 == null ){
                                    return -1;
                                }
                                if(val2 == null){
                                    return 1;
                                }
                                int res = Double.compare(val1, val2);
                                if(res == 0){
                                    continue;
                                }
                                else{
                                    return res;
                                }

                            }
                        }
                    }

                }
            }
        }
        return 1; // in the case of an exact tie.
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
        //create copy array in case of modification
        ArrayList<Integer> pageList = new ArrayList<Integer>(table.getPageOrder());
        //iterate over
        for (Integer pageNum : pageList){
            byte[] bytes =  bm.getPage(table.getFileName(), pageNum);
            Page page = new Page(pageNum, table, bm.getPageSize(), bytes);
            ArrayList<Integer> deletedIndex = new ArrayList<Integer>();

            //go through record
            for (int index = 0; index < records.size(); index++) {
                if (page.belongs(records.get(index).getKey())) {
                    //remove from table
                    page.removeRecord(records.get(index).getKey());
                    //add to deleted
                    deletedIndex.add(index);
                }
            }

            //delete from record
            Collections.sort(deletedIndex, Collections.reverseOrder());
            for (int index : deletedIndex) {
                records.remove(index);
            }

            //write to buffer
            bm.writePage(table.getFileName(), pageNum, page.getBytes());
        }

        //check if any records left
        if (records.size() > 0) {
            return records.size() + " records not deleted.\nERROR";  //returns an error if not all records updated
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
        //get schema
        Schema table = c.getSchema(tableName);

        //check if target is correct
        boolean found = false;
        for (Attribute attribute : table.getAttributes()) {
            if (target.equals(attribute.getName())) {
                found = true;
            }
        }
        if (!found) {
            return target + " is not an element in " + tableName + ".\nERROR";  //returns an error
        }
        //get attribute
        Attribute targetAttribute = table.getAttribute(target);

        //check if null
        if (value.toString().equals("null")) {
            value = null;
        }
        //check if value is right type
        else if (targetAttribute.getType().contains("char")) {
            //varchar or char
            try {
                value = value.toString();
            } catch(Exception e) {
                return value.toString() + " is not a string, but should be.\nERROR";  //returns an error
            }
        }
        else if (targetAttribute.getType().equalsIgnoreCase("integer")) {
            //integer
            try {
                value = Integer.parseInt(value.toString());
            } catch(Exception e) {
                return value.toString() + " is not an integer, but should be.\nERROR";  //returns an error
            }
        }
        else if (targetAttribute.getType().equalsIgnoreCase("boolean")) {
            //boolean
            if (value.toString().toLowerCase().equals("true")) {
                value = (boolean) true;
            }
            else if (value.toString().toLowerCase().equals("false")) {
                value = (boolean) false;
            }
            else {
                return value.toString() + " is not a boolean, but should be.\nERROR";  //returns an error
            }
        }
        else {
            //double
            try {
                value = Double.parseDouble(value.toString());
            } catch(Exception e) {
                return value.toString() + " is not a double, but should be.\nERROR";  //returns an error
            }
        }

        //iterate through records
        for (Record record : records) {
            ArrayList<Integer> pageList = new ArrayList<Integer>();
            //create copy array in case of modification
            for (Integer pageNum : table.getPageOrder()) {
                pageList.add(pageNum);
            }

            //iterate through each page
            for (int pageNum : pageList) {
                //get page
                byte[] bytes =  bm.getPage(table.getFileName(), pageNum);
                Page page = new Page(pageNum, table, bm.getPageSize(), bytes);
                ArrayList<Integer> deletedIndex = new ArrayList<Integer>();

                if (page.belongs(record.getKey())) {
                    //delete record
                    ArrayList<Record> delete = new ArrayList<Record>();
                    delete.add(record);
                    deleteRecords(delete, tableName);

                    // into tuples
                    ArrayList<String> tuple = new ArrayList<String>();
                    Map<String, Object> recordValues = record.getAttributes();
                    for (Attribute attribute : table.getAttributes()) {
                        if (attribute.getName().equalsIgnoreCase(target)) {
                            if (value == null) {
                                tuple.add("null");
                            }
                            else {
                                tuple.add(value.toString());
                            }
                        }
                        else {
                            tuple.add(recordValues.get(attribute.getName()).toString());
                        }
                    }
                    ArrayList<ArrayList<String>> tuples = new ArrayList<ArrayList<String>>();
                    tuples.add(tuple);

                    //attempt to insert into table
                    if (insert(tableName, tuples).contains("ERROR")) {
                        tuples.clear();
                        tuple.clear();

                        //rollback change
                        for (Attribute attribute : table.getAttributes()) {
                            tuple.add(recordValues.get(attribute.getName()).toString());
                        }
                        tuples.add(tuple);
                        insert(tableName, tuples);
                        //return error
                        return "conflict in update. \nERROR";
                    }
                    break;
                }
            }
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

        Schema new_table = new Schema(name, attributes, c.getBufferManager());
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
