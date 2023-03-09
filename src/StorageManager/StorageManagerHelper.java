package src.StorageManager;


import src.Catalog.Attribute;
import src.Catalog.Schema;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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
            System.out.println("Expected " + tableAttributes.size() + "attributes, got "
                    + tuple.size() + "attributes.");
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
            if(tblType.contains("char")){
                int length = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(") + 1, tblType.indexOf(")")).strip());
                checkRes = checkChar(length, tupleAttribute, notNull, atrName, attributes);
            }
            else if(tblType.contains("varchar")){
                int length = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(") + 1, tblType.indexOf(")")).strip());
                checkRes = checkVarChar(length, tupleAttribute, notNull, atrName, attributes);
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
                if(!checkPrimaryKey(table, tupleAttribute, bm)){
                    System.out.println("Non-unique or null primarykey: " +
                            tupleAttribute);
                    return null;
                }
            }
            if(unique){
                if(!checkUniqueness(table, tupleAttribute, bm)){
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
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                attributes.put(atrName, null);
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength == length){
                attributes.put(atrName, tupAttr);
                return null;
            }
            return atrName + " Expected a char of length " + length
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
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                attributes.put(atrName, null);
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength <= length){
                attributes.put(atrName, tupAttr);
                return null;
            }
            return atrName + " Expected a varChar of length " + length +
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
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                attributes.put(atrName, null);
                return null;
            }
            if(tupAttr.equals("true") || tupAttr.equals("false")){
                boolean tupBool = Boolean.parseBoolean(tupAttr);
                attributes.put(atrName, tupBool);
                return null;
            }
            else{
                return atrName + " Expected a boolean value, but got: " + tupAttr;
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
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                attributes.put(atrName, null);
                return null;
            }
            try{
                Double tupDouble = Double.parseDouble(tupAttr);
                attributes.put(atrName, tupDouble);
                return null;
            }
            catch(NumberFormatException e){
                return atrName + " Expected an integer value, but got: " + tupAttr;
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
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                attributes.put(atrName, null);
                return null;
            }
            try{
                int tupInteger = Integer.parseInt(tupAttr);
                attributes.put(atrName, tupInteger);
                return null;
            }
            catch(NumberFormatException e){
                return atrName + " Expected an integer value, but got: " + tupAttr;
            }
        }
    }


    /**
     * Checks whether a non primary key is unique in its column.
     *
     * @param table The table whose column is being checked.
     * @param obj The value whose being checked for uniqueness.
     * @param bm The buffer manager for the database.
     * @return True if the obj  is unique in the column, false otherwise.
     */
    private static boolean checkUniqueness(Schema table, Object obj, BufferManager bm){
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
                    if (rec.getAttributes().containsValue(obj)) {
                        return false;
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
        if (table.getPages() > 0) {
            ArrayList<Integer> pgOrder = table.getPageOrder();
            String fileName = table.getFileName();
            for (Integer i : pgOrder) {
                Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
                if (pg.belongs(obj)) {
                    if (pg.getRecord(obj) != null) {
                        return true;
                    }
                } else {
                    System.out.println("CheckAttributes error, passed a obj that doesn't belong.");
                    return false;
                }
            }
        }
        return true;
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
}
