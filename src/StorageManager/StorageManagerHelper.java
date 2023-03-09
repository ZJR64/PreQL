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
     * @param tuples
     * @return
     */
    public static String checkAttributes(Schema table, ArrayList<String> tuple){
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



            if(tblType.contains("char")){ // val must be char
                int charSize = Integer.parseInt(tblType.substring(
                        tblType.indexOf("(")+1, tblType.indexOf(")")).strip());
                if(tupleAttribute.length() != charSize){
                    return
                }
                if(unique){
                    if(checkUniqueness(table, ))
                }
                if(notNull){

                }
                if(primaryKey){

                }
            }
            else if(tblType.contains("varchar")){ // val must be varchar

                if(unique){

                }
                if(notNull){

                }
                if(primaryKey){

                }
            }
            else if(tblType.contains("boolean")) { // val must be bool
                if(unique){

                }
                if(primaryKey){

                }
                if(notNull){

                }
                else{

                }

            }
            else if (tblType.contains("integer")){ // val must be int
                if(unique){

                }
                if(primaryKey){

                }
                if(notNull){

                }

            }
            else if(tblType.contains("double")){ // val must be double
                if(unique){

                }
                if(primaryKey){

                }
                if(notNull){

                }
            }


        }

        return null;
    }

    private String checkUniqueness(Schema table,Object obj){
        for(){

        }
        return null;
    }
}
