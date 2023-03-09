package src.StorageManager;


import src.Catalog.Attribute;
import src.Catalog.Schema;

import java.util.ArrayList;

/**
 * Holds the many functions needed exclusively by StorageManager.
 */
public class StorageManagerHelper {


    /**
     * Checks the given tuple
     * @param table
     * @param 
     * @return
     */
    public static String checkAttributes(Schema table, ArrayList<String> tuple, BufferManager bm){
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        if(tuple.size() != tableAttributes.size()){
            return "Expected " + tableAttributes.size() + "attributes, got "
                    + tuple.size() + "attributes.";
        }

        for(int i = 0; i < tableAttributes.size(); i++){
            Attribute tblAttribute = tableAttributes.get(i);
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


            if(tblType.contains("char")){

            }
            else if(tblType.contains("varchar")){

            }
            else if(tblType.contains("boolean")) {


            }
            else if (tblType.contains("integer")){


            }
            else if(tblType.contains("double")){

            }


        }

        return null;
    }


    /**
     * Checks whether a non primary key is unique in its column.
     *
     * @param table The table whose column is being checked.
     * @param obj The value whose being checked for uniqueness.
     * @param bm The buffer manager for the database.
     * @return True if the obj  is unique in the column, false otherwise.
     */
    private static boolean checkUniquness(Schema table, Object obj, BufferManager bm){
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
