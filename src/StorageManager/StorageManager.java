package src.StorageManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.sun.org.apache.xpath.internal.operations.Bool;
import src.Catalog.*;


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
     * @return If the insertion was successful or not.
     */
    public String insert(String tableName, ArrayList<ArrayList<String>> tuples) {
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }

        for(ArrayList<String> tuple : tuples){  // for tuple in tuples, for loop will create tuples into records.
            String checkResult = StorageManagerHelper.checkAttributes(table, tuple, bm);
            if(checkResult == null){ // we're good, the tuple is valid and we can make the record.

            }
            else{
                return checkResult + "\nERROR";
            }
        }
        return null;
    }


    /**
     * Gets a record by using its primary key.
     */
    public void getRecord(){

    }

    /**
     * Gets a page by it's table and page number.
     */
    public void getPage(){

    }

    /**
     *
     * @param tableName
     * @return
     */
    public String getAllRecords(String tableName){
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");  //returns an error if there is no table
        }
        if(table.getPages() == 0){
            return makeAttributesString(table).concat("\nSUCCESS"); //returns the attributes of the table
        }
        System.out.println(makeAttributesString(table));                //print out the attributes

        for(int i = 0; i < table.getPages(); i++){                      //for each page in order
            byte[] page = bm.getPage(tableName, i);                     //gets the next page
            System.out.println(page);                                      //TODO test this print


            int numOfRecs = 0;                                          //Initialize the number of records
            int index = 0;                                              //Index of the page
            while (index < Integer.SIZE/8) {
                numOfRecs = (numOfRecs << 8) + (page[index] & 0xFF);    //turns the first couple bytes into the number of records
                index++;
            }
            index += Integer.SIZE/8;
            for (int recordNum = 0; recordNum < numOfRecs; recordNum++){
                //iterates through each record of the current page
                String recordOutput = "| ";                             //Initialize the string of the record

                for (Attribute att:table.getAttributes()) {

                    int offset = 0;                                         //The offset of the
                    while (index < Integer.SIZE/8) {
                        offset = (offset << 8) + (page[index] & 0xFF);
                        index++;
                    }
                    int size = 0;                                           //The size of the record
                    while (index < Integer.SIZE/8) {
                        size = (size << 8) + (page[index] & 0xFF);
                        index++;
                    }
                    String type = att.getType();                                    //get the type of the attribute
                    if (type.equals("integer")) {
                        int intValue = 0;                                           //The size of the record
                        for (int byteNum = 0; byteNum < Integer.SIZE/8; byteNum++) {
                            intValue = (intValue << 8) + (page[offset + byteNum] & 0xFF);
                        }
                        String tempString = Integer.toString(intValue);
                        tempString = String.format("%1$"+15+ "s", tempString);
                        recordOutput = recordOutput + tempString + " | ";

                    }
                    else if (type.equals("double")) {
                        byte[] bytes = new byte[Double.SIZE/8];
                        double doubleValue = 0;                             //The size of the record
                        for (int byteNum = 0; byteNum < Double.SIZE/8; byteNum++) {
                            bytes[byteNum] = page[offset + byteNum];
                        }
                        int attributeSize = att.getSize();
                        doubleValue = ByteBuffer.wrap(bytes).getDouble();
                        String tempString = Double.toString(doubleValue);
                        tempString = String.format("%1$"+20+ "s", tempString);
                        recordOutput = recordOutput + tempString + " | ";

                    }
                    else if (type.equals("boolean")) {
                        boolean boolValue = false;
                        int temp = 0;
                        temp = (page[offset] & 0xFF);
                        boolValue = Boolean.valueOf(Integer.toString(temp));
                        recordOutput = recordOutput + boolValue + " | ";

                        if (boolValue == false){
                            recordOutput = recordOutput + "false | ";
                        }
                        else{
                            recordOutput = recordOutput + "true  | ";
                        }

                    }
                    else if (type.startsWith("char")) {
                        int charAmount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                        String outputString = "";
                        for (int byteNum = 0; byteNum < charAmount; byteNum++) {
                            outputString = outputString +  (char) page[offset + byteNum];
                        }
                        recordOutput = recordOutput + outputString + " | ";

                    }
                    else if (type.startsWith("varchar")) {
                        String outputString = "";
                        for (int byteNum = 0; byteNum < size; byteNum++) {
                            outputString = outputString +  (char) page[offset + byteNum];
                        }
                        recordOutput = recordOutput + outputString + " | ";

                    }
                }
                System.out.println(recordOutput + "\n");

            }

        }

        return "SUCCESS";
    }


    /**
     * Makes a string containing nicely formatted attributes.
     * @param table The table the attributes are pulled from.
     * @return The completed attributes string.
     */
    public String makeAttributesString(Schema table){
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
                topStr.append("-");                  // "--------"
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
     * Deletes a record from a given table using the given primary key.
     */
    public void deleteRecord(){

    }

    /**
     * Updates a record in a given table using the given primary key.
     */
    public void updateRecord(){

    }

    public String createTable(String name, ArrayList<Attribute> attributes){
        for (Schema i : c.getSchemas()) {
            if(i.getName().equals(name)){
                return "Table already exists" +
                        "\nERROR";
            }
        }

        Schema new_table = new Schema(name, attributes);
        c.schemas.add(new_table);
        return "Success";
    }
}
