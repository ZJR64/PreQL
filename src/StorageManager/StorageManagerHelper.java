package src.StorageManager;


import src.Catalog.Attribute;
import src.Catalog.Schema;

import java.util.ArrayList;

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
     * @param bm the buffer manager for the databse.
     * @return null if the tuple is correct, an error string otherwise.
     */
    public static String checkAttributes(Schema table, ArrayList<String> tuple, BufferManager bm){
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        if(tuple.size() != tableAttributes.size()){
            return "Expected " + tableAttributes.size() + "attributes, got "
                    + tuple.size() + "attributes.";
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
                checkRes = checkChar(length, tupleAttribute, notNull, atrName);
            }
            else if(tblType.contains("varchar")){
                int length = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(") + 1, tblType.indexOf(")")).strip());
                checkRes = checkVarChar(length, tupleAttribute, notNull, atrName);
            }
            else if(tblType.contains("boolean")) {
                checkRes = checkBoolean(tupleAttribute, notNull, atrName);
            }
            else if (tblType.contains("integer")){
                checkRes = checkInteger(tupleAttribute, notNull, atrName);
            }

            else if(tblType.contains("double")){
                checkRes = checkDouble(tupleAttribute, notNull, atrName);
            }
            else{
                return "tblType did not match any type, something wrong in checkAttributes.";
            }

            if(checkRes != null){
                return checkRes;
            }
            if(primaryKey){
                if(!checkPrimaryKey(table, tupleAttribute, bm)){
                    return "Non-unique or null primarykey: " + tupleAttribute;
                }
            }
            if(unique){
                if(!checkUniqueness(table, tupleAttribute, bm)){
                    return "Non-unique attribute: " + tupleAttribute;
                }
            }


        }
        return null;
    }


    /**
     * Checks the tuple's value to see if is the correct length, and enforces
     * notNull on the value if notNull is true.
     *
     * @param length The length the tuple's value must be.
     * @param tupAttr The value whose length is being checked.
     * @param notNull Tells whether this value can be null or not.
     * @param atrName Name of the attribute whose length is being enforced.
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkChar(int length, String tupAttr,
                                    boolean notNull, String atrName){
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength == length){
                return null;
            }
            return atrName + " Expected a char of length " + length + ", but got a char of length "
                    + attrLength;
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
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkVarChar(int length, String tupAttr,
                                       boolean notNull, String atrName){
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                return null;
            }

            int attrLength = tupAttr.length();
            if(attrLength <= length){
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
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkBoolean(String tupAttr, boolean notNull,
                                       String atrName){
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                return null;
            }
            if(tupAttr.equals("true") || tupAttr.equals("false")){
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
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkInteger(String tupAttr, boolean notNull,
                                       String atrName){
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                return null;
            }
            try{
                Double.parseDouble(tupAttr);
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
     * @return Either a string of null if the char passes, or an error message.
     */
    private static String checkDouble(String tupAttr, boolean notNull,
                                      String atrName){
        if(tupAttr == null && notNull){
            return atrName + " Cannot have null values";
        }
        else{
            if(tupAttr == null){
                return null;
            }
            try{
                Integer.parseInt(tupAttr);
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
        ArrayList<Integer> pgOrder = table.getPageOrder();
        String fileName = table.getFileName();
        for(Integer i : pgOrder){
            Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
            ArrayList<Record> recs = pg.getRecords();
            for(Record rec : recs){
                if(rec.getAttributes().containsValue(obj)){
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Checks whether the passed in priamry key is unique in its column.
     *
     * @param table The table whose column is being checked.
     * @param obj The primary key whose being checked for uniqueness.
     * @param bm The buffer manager for the database.
     * @return True if primarykey is unique in the column, false otherwise.
     */
    private static boolean checkPrimaryKey(Schema table, Object obj, BufferManager bm){
        if(obj == null){
            return false;
        }
        ArrayList<Integer> pgOrder = table.getPageOrder();
        String fileName = table.getFileName();
        for(Integer i : pgOrder){
             Page pg = new Page(i, table, bm.getPageSize(), bm.getPage(fileName, i));
             if(pg.belongs(obj)){
                 if(pg.getRecord(obj) != null){
                     return false;
                 }
             }
             else{
                 System.out.println("CheckAttributes error, passed a obj that doesn't belong.");
                 return false;
             }
        }
        return true;
    }
}
