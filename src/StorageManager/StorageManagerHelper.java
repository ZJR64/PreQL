package src.StorageManager;


import src.Catalog.Attribute;
import src.Catalog.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Holds the many functions needed by StorageManager.
 */
public class StorageManagerHelper {


    /**
     * Checks a tuple to see if all values meet all constraints, are of correct
     * type, and if there is a correct number of values in the tuple.
     *
     * @param table the table the values are being checked against.
     * @param tuple the tuple being checked.
     * @param bm the buffer manager for the database.
     * @return null if the tuple is invalid, a map containing the attributes otherwise.
     */
    public static Map<String, Object> checkAttributes(Schema table, ArrayList<String> tuple, BufferManager bm){
        Map<String, Object> attributes = new HashMap<>();
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        if(tuple.size() != tableAttributes.size()){
            System.out.println("Expected " + tableAttributes.size() + " attributes, got "
                    + tuple.size() + " attributes.");
            return null;
        }

        for(int i = 0; i < tableAttributes.size(); i++){
            Attribute tblAttribute = tableAttributes.get(i);
            String atrName = tblAttribute.getName();
            String tblType = tblAttribute.getType();

            ArrayList<String> descriptors = tblAttribute.getDescriptors();
            boolean notNull = false;    //true if attribute must be not null
            boolean unique = false;     //true if attribute must be unique
            boolean primaryKey = false; //true if attribute is a PK
            for(String descriptor : descriptors){
                switch (descriptor) {
                    case "unique" -> unique = true;
                    case "notnull" -> notNull = true;
                    case "primarykey" -> primaryKey = true;
                }
            }

            String tupleAttribute = tuple.get(i);

            String checkRes;
            if(tblType.contains("varchar")){
                int length = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(") + 1, tblType.indexOf(")")).strip());
                checkRes = checkVarChar(length, tupleAttribute, notNull, atrName, attributes);
            }
            else if(tblType.contains("char")){
                int length = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(") + 1, tblType.indexOf(")")).strip());
                checkRes = checkChar(length, tupleAttribute, notNull, atrName, attributes);
            }
            else if(tblType.contains("boolean")) {
                checkRes = checkBoolean(tupleAttribute, notNull, atrName, attributes);
            }
            else if (tblType.contains("integer")){
                checkRes = checkInteger(tupleAttribute, notNull, atrName, attributes);
            }

            else if(tblType.contains("double")){
                checkRes = checkDouble(tupleAttribute, notNull, atrName, attributes);
            }
            else{
                System.out.println("tblType did not match any type, " +
                        "something wrong in checkAttributes.");
                return null;
            }

            if(checkRes != null){
                System.out.println(checkRes);
                return null;
            }
            if(primaryKey){
                if(!checkPrimaryKey(table, attributes.get(atrName) , bm)){
                    System.out.println("Non-unique or null primarykey: " +
                            tupleAttribute);
                    return null;
                }
            }
            if(unique){
                if(!checkUniqueness(table, attributes.get(atrName), bm, atrName)){
                    System.out.println("Non-unique attribute: " +
                            tupleAttribute);
                    return null;
                }
            }

        }
        return attributes;
    }


    /**
     * Checks the tuple's value to see if is the correct length, and enforces
     * notNull on the value if notNull is true.
     *
     * @param length The length the tuple's value must be.
     * @param tupAttr The value whose length is being checked.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @param attributes The map of attributes that might be added to.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkChar(int length, String tupAttr,
                                    boolean notNull, String atrName,
                                    Map<String, Object> attributes){
        if(tupAttr.equals("null") && notNull){
            return "Attribute " + atrName + " Cannot have null values";
        }
        else{
            if(tupAttr.equals("null")){
                attributes.put(atrName, null);
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength == length){
                attributes.put(atrName, tupAttr);
                return null;
            }
            return "Attribute " + atrName + " Expected a char of length " + length
                    + ", but got a char of length " + attrLength;
        }
    }

    /**
     * Checks the tuple's value to see if is the correct length, and enforces
     * notNull on the value if notNull is true.
     *
     * @param length The length the tuple's value must less than or equal to.
     * @param tupAttr The value whose length is being checked.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @param attributes The map of attributes that might be added to.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkVarChar(int length, String tupAttr,
                                       boolean notNull, String atrName,
                                       Map<String, Object> attributes){
        if(tupAttr.equals("null") && notNull){
            return "Attribute " + atrName + " Cannot have null values";
        }
        else{
            if(tupAttr.equals("null")){
                attributes.put(atrName, null);
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength <= length){
                attributes.put(atrName, tupAttr);
                return null;
            }
            return "Attribute " + atrName + " Expected a varChar of length " + length +
                    " or less, but got a char of length " + attrLength;
        }
    }


    /**
     * Checks the tuple's value to see if is a boolean, and enforces
     * notNull on the value if notNull is true.
     *
     * @param tupAttr The value being checked if it is a boolean.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @param attributes The map of attributes that might be added to.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkBoolean(String tupAttr, boolean notNull,
                                       String atrName, Map<String, Object> attributes){
        if(tupAttr.equals("null") && notNull){
            return "Attribute " + atrName + " Cannot have null values";
        }
        else{
            if(tupAttr.equals("null")){
                attributes.put(atrName, null);
                return null;
            }
            if(tupAttr.equals("true") || tupAttr.equals("false")){
                boolean tupBool = Boolean.parseBoolean(tupAttr);
                attributes.put(atrName, tupBool);
                return null;
            }
            else{
                return "Attribute " + atrName + " Expected a boolean value, but got: " + tupAttr;
            }
        }
    }

    /**
     * Checks the tuple's value to see if is an integer, and enforces
     * notNull on the value if notNull is true.
     *
     * @param tupAttr The value being checked if it is an integer.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @param attributes The map of attributes that might be added to.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkInteger(String tupAttr, boolean notNull,
                                       String atrName, Map<String, Object> attributes){
        if(tupAttr.equals("null") && notNull){
            return "Attribute " + atrName + " Cannot have null values";
        }
        else{
            if(tupAttr.equals("null")){
                attributes.put(atrName, null);
                return null;
            }
            try{
                int tupInteger = Integer.parseInt(tupAttr);
                attributes.put(atrName, tupInteger);
                return null;
            }
            catch(NumberFormatException e){
                return "Attribute " + atrName + " Expected an integer value, but got: " + tupAttr;
            }
        }
    }


    /**
     * Checks the tuple's value to see if is a double, and enforces
     * notNull on the value if notNull is true.
     *
     * @param tupAttr The value being checked if it is a double.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @param attributes The map of attributes that might be added to.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkDouble(String tupAttr, boolean notNull,
                                      String atrName, Map<String, Object> attributes){
        if(tupAttr.equals("null") && notNull){
            return "Attribute " + atrName + " Cannot have null values";
        }
        else{
            if(tupAttr.equals("null")){
                attributes.put(atrName, null);
                return null;
            }
            try{
                Double tupDouble = Double.parseDouble(tupAttr);
                attributes.put(atrName, tupDouble);
                return null;
            }
            catch(NumberFormatException e){
                return "Attribute " + atrName + " Expected an integer value, but got: " + tupAttr;
            }
        }
    }


    /**
     * Checks whether a non primary key is unique in its column.
     *
     * @param table The table whose column is being checked.
     * @param obj The value whose being checked for uniqueness.
     * @param bm The buffer manager for the database.
     * @param atrName
     * @return True if the obj  is unique in the column, false otherwise.
     */
    private static boolean checkUniqueness(Schema table, Object obj,
                                           BufferManager bm, String atrName){
        if(obj == null){
            return true;
        }
        if(table.getPages() > 0) {
            ArrayList<Integer> pgOrder = table.getPageOrder();
            String fileName = table.getFileName();
            for (Integer i : pgOrder) {
                Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                ArrayList<Record> recs = pg.getRecords();
                for (Record rec : recs) {
                    Map<String, Object> attributes = rec.getAttributes();
                    for(Map.Entry<String, Object> entry : attributes.entrySet()){
                        if(entry.getKey().equals(atrName)){
                            if(entry.getValue().equals(obj)){
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Checks whether the passed in primary key is unique in its column.
     *
     * @param table The table whose column is being checked.
     * @param obj The primary key whose being checked for uniqueness.
     * @param bm The buffer manager for the database.
     * @return True if primarykey is unique in the column, false otherwise.
     */
    private static boolean checkPrimaryKey(Schema table, Object obj, BufferManager bm) {
        if (obj == null) {
            return false;
        }
        //for first record
        if (table.getPages() < 1) {
            return true;
        }
        ArrayList<Integer> pgOrder = table.getPageOrder();
        String fileName = table.getFileName();
        for (Integer i : pgOrder) {
            Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
            if (pg.belongs(obj)) {
                if (pg.getRecord(obj) == null) {
                    return true;
                }else {
                    break;
                }
            }
        }
        System.out.println("CheckAttributes error, passed a obj that doesn't belong.");
        return false;
    }


    /**
     * Makes a string containing nicely formatted attributes.
     *
     * @param table The table the attributes are pulled from.
     * @return The completed attributes string.
     */
    public static String makeAttributesString(Schema table){
        ArrayList<Attribute> attributes = table.getAttributes();
        StringBuilder str = new StringBuilder();
        str.append("\n");
        StringBuilder topStr = new StringBuilder();
        StringBuilder midStr = new StringBuilder();
        StringBuilder botStr = new StringBuilder();
        for(int i = 0; i < attributes.size(); i++){
            Attribute atr = attributes.get(i);
            String atrName = atr.getName();
            int atrSize = atrName.length();
            int typeSize = 0;
            String type = atr.getType();
            if (type.equals("integer")) {
                typeSize = 15;
            }
            else if (type.equals("double")) {
                typeSize = 20;
            }
            else if (type.equals("boolean")) {
                typeSize = 5;
            }
            else {
                String size = type;
                size = size.substring(size.indexOf("(") + 1);
                size = size.substring(0, size.indexOf(")"));
                typeSize = Integer.parseInt(size);
            }

            //check what is bigger
            if (atrSize < typeSize) {
                atrSize = typeSize;
            }

            for(int j = 0; j < atrSize + 4; j++){ // attribute "size" would make
                topStr.append("-");               // "--------"
            }
            String bottomBox = str.toString();
            if(i == 0){
                midStr.append("| ");
            }
            atrName = String.format("%1$"+atrSize+ "s", atrName);
            midStr.append(atrName);
            midStr.append(" | ");
        }
        botStr.append(topStr);
        str.append(topStr);
        str.append("\n");
        str.append(midStr);
        str.append("\n");
        str.append(botStr);
        return str.toString();
    }


    /**
     * Attempts to add an attribute to a table.
     *
     * @param schema The table being added to.
     * @param attributeName The name of the attribute being added.
     * @param attributeType The type of the attribute being added.
     * @param defaultValue The default value of the attribute, null if not specified.
     * @param bm the buffer manager for the table.
     * @return A string reporting success or failure.
     */
    public static String alterAdd(Schema schema, String attributeName,
                                  String attributeType, String defaultValue, BufferManager bm) {
        ArrayList<Attribute> tableAttributes = schema.getAttributes();
        for(Attribute attr : tableAttributes){
            if(attr.getName().equals(attributeName)){
                return "Table " + schema.getName() + "already has an attribute with name " +
                        attributeName + "\nERROR";

            }
        }
        Attribute newAttribute;
        ArrayList<String> descriptors = new ArrayList<String>(); // Will never have descriptors.
        Object newDef = null;
        if(attributeType.equals("integer")){
            newAttribute = new Attribute(attributeType, Integer.SIZE/Byte.SIZE, attributeName, descriptors);
            try {
                newDef = Integer.parseInt(defaultValue);
            }
            catch(NumberFormatException e){
                return attributeType + " given invalid default value. \nERROR";
            }
            catch(NullPointerException e){
                newDef = null;
            }

        }
        else if(attributeType.equals("double")){
            newAttribute = new Attribute(attributeType, Double.SIZE/8, attributeName, descriptors);
            try {
                newDef = Double.parseDouble(defaultValue);
            }
            catch(NumberFormatException e){
                return attributeType + " given invalid default value. \nERROR";
            }
            catch(NullPointerException e){
                newDef = null;
            }
        }
        else if(attributeType.equals("boolean")){
            newAttribute = new Attribute(attributeType, 1, attributeName, descriptors);
            if(defaultValue != null){
                if(defaultValue.equals("true")){
                    newDef = true;
                }
                else if(defaultValue.equals("false")){
                    newDef = false;
                }
                else{
                    return attributeType + " given invalid default value. \nERROR";
                }
            }

        }
        else if(attributeType.contains("char")){ //handles char and varchar
            try {
                int length = Integer.parseInt(attributeType.substring(
                        attributeType.indexOf("(") + 1, attributeType.indexOf(")")).strip());
                newAttribute = new Attribute(attributeType, (Character.SIZE*length)/8, attributeName, descriptors);
                newDef = defaultValue;
            }
            catch(NumberFormatException e){
                return attributeType + " given invalid size. \nERROR";
            }
        }
        else{
            return attributeType + " is not a valid data type. \nERROR";
        }
        alterCreateReplacement(schema, newAttribute, newDef, bm, true);
        return "SUCCESS";
    }


    /**
     * Attempts to drop an attribute from a table.
     *
     * @param schema The table being removed from.
     * @param attributeName The name of the attribute being dropped.
     * @param bm the buffer manager for the table.
     * @return A string reporting success or failure.
     */
    public static String alterDrop(Schema schema, String attributeName, BufferManager bm){
        ArrayList<Attribute> tableAttributes = schema.getAttributes();
        Attribute toRemove = null;
        for(Attribute attr : tableAttributes){
            if(attr.getName().equals(attributeName)){
                toRemove = attr;
            }
        }
        if(toRemove == null){ // check if attribute exists.
            return "Table " + schema.getName() + "does not have attribute " +
                    attributeName + "\n ERROR";
        }
        if(toRemove.getDescriptors().contains("primarykey")){  // check if attribute is a primarykey.
            return "Cannot remove a primary key attribute \nERROR";
        }
        alterCreateReplacement(schema, toRemove, null, bm, false);
        return "SUCCESS";
    }


    /**
     * Makes a new copy of a table except it either has an added or removed
     * attribute, copies over data into the table, and deletes the old table.
     *
     * @param schema The table being copied.
     * @param attribute An attribute being added or removed from the table.
     * @param defaultValue The value to be filled in when an attribute is being added.
     * @param bm The buffer manager for the database.
     * @param addOrDrop if true, adding, if false, dropping.
     */
    private static void alterCreateReplacement(Schema schema, Attribute attribute, Object defaultValue,
                                               BufferManager bm, boolean addOrDrop){

        ArrayList<Integer> pgOrder = schema.getPageOrder();

        ArrayList<Record> newRecs = new ArrayList<>();

        for(int pgNum : pgOrder){  // for each page in the old table

            Page pg = new Page (pgNum, schema, bm.getPageSize(), bm.getPage(schema.getFileName(), pgNum)); // make a pg struct
            ArrayList<Record> records = pg.getRecords(); // get the records in that page.

            for(Record rec : records){ // for each record in that page

                if(addOrDrop) { // doing add
                    rec.addAttribute(attribute.getName(), defaultValue); // add the attribute and its defaultValue(null if not specified).
                }
                else{ // doing drop
                    rec.removeAttribute(attribute.getName());
                }

                //add to newRecs
                newRecs.add(rec);
            }
        }

        if(addOrDrop){ // doing add
            schema.addAttribute(attribute);
        }
        else{ // doing drop
            schema.deleteAttribute(attribute);
        }

        String fileName = schema.getFileName();
        //delete pages
        bm.purge(fileName);
        schema.clearPages();

        //create first page
        bm.addPage(fileName, schema.getOpenPages());
        Page newpg = new Page(0, schema, bm.getPageSize(), 0);
        bm.writePage(fileName, 0, newpg.getBytes());

        //add to records
        for (Record rec : newRecs) {
            for (Integer i : pgOrder) {
                //get page from buffer
                Page pg = new Page(i, schema, bm.getPageSize(), bm.getPage(fileName, i));
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
    }
}
