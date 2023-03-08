package src.StorageManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import src.Catalog.*;


/**
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class StorageManager {

    public static BufferManager bm;
    private static Catalog c;

    public StorageManager(BufferManager bm, Catalog c){
        this.bm = bm;
        this.c = c;
    }

    /**
     * Inserts into a table a number of records.
     *
     * @param tableNThe table being inserted into
     * @param tuples the records to be inserted into the table.
     * @return If the insertion was successful or not.
     */
    public String insert(String tableN, ArrayList<ArrayList<String>> tuples) {
        String tableName = tableN;
        Schema table = c.getSchema(tableName);
        if(table == null){
            return "No such table " + tableName.concat("\nERROR");
        }
        ArrayList[] result = checkAttributes(table, tuples);
        if(result == null){  // if result equals null, attributes are not good.
            return "ERROR";
        }
        ArrayList<Object> values = result[0];
        ArrayList<ArrayList<Integer>> sizes = result[1];
        ArrayList<byte[]> records = new ArrayList<>();
        try{
            records = convertToBytes(tuples, sizes, values, table);
            if(records.isEmpty()){
                System.err.println("Error: Could not write out bytes");
                return "FILE ERROR";
            }
        }
        catch(Exception e){
            System.err.println("Error: tuples could not be read");
            return "FILE ERROR";
        }
        //if table doesnt yet have pages.
        if(table.getPages() == 0){
            int pageNum = 0;
            for(int i = 0; i < records.size(); i++){
                if(i == 0){
                    pageNum = bm.addPage(tableName, table.getOpenPages());
                    table.addRecord();
                    table.addPage(0, 0);
                }
                bm.writePage(tableName, pageNum, records.get(i));

            }
           return "SUCCESS";
        }
        int page_num = bm.getPageSize() * 0;
        byte[] page = bm.getPage(tableName, page_num);
        int freeSpace = page[0];
        for(int i = 0; i < records.size(); i++){
            byte[] record = records.get(i);
            for(int j = 0; j < record.length; j++){
                page[freeSpace + i] = record[j];
            }
        }
        return "SUCCESS";
    }

    private ArrayList<byte[]> split(){

        return null;
    }


    /**
     * Checks if the user is trying to insert the correct number and types of
     * values into the table.
     *
     * @param table the table that contains the attributes being checked against.
     * @param tuples the tuples containing the attributes being checked
     * @return null if the tuples are correct, otherwise a string explaining
     * what is wrong with the string.
     */
    private ArrayList[] checkAttributes(Schema table,
                                        ArrayList<ArrayList<String>> tuples){
        ArrayList[] toReturn = new ArrayList[tuples.size() + 1];
        ArrayList<Object> values = new ArrayList<>();
        ArrayList<ArrayList<Integer>> allRecordsAttributeSizes = new ArrayList<>();
        ArrayList<Attribute> tableAttributes = table.getAttributes();
        for(ArrayList<String> tuple: tuples){                // for each tuple
            ArrayList<Integer> recordAttributeSizes = new ArrayList<>();
            if(tuple.size() > tableAttributes.size()){  // check if too many attributes
                 System.out.println("row (" + tuple.toString()+ ")" +
                        "Too few attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString());
            }
            else if(tuple.size() < tableAttributes.size()){ //check if enough attributes
                System.out.println("row (" + tuple.toString()+ ")" +
                        "Too many attributes: expected  " +
                        table.getAttributes().toString() +
                        " got " + tuple.toString());
            }
            for(int i = 0; i < tableAttributes.size(); i++){ // for each attribute in the table
                Attribute attr = tableAttributes.get(i); // get table attribute i
                String type = attr.getType();            // get type of attribute i
                String val = tuple.get(i);

                if(type.startsWith("varchar")){
                    if(val == null){
                        recordAttributeSizes.add(1);
                        values.add(null);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() <= amount){
                        values.add(val);
                        recordAttributeSizes.add(val.length());
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    else{
                         System.out.println("TOO BIG ERROR");
                        return null;
                    }

                }
                else if(type.startsWith("char")){
                    if(val == null){
                        values.add(null);
                        recordAttributeSizes.add(1);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
                    if(val.length() == amount){
                        values.add(val);
                        recordAttributeSizes.add(val.length());
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    else{
                        System.out.println("WRONG SIZE error");
                        return null;
                    }
                }
                else if(type.equals("double")){
                    if(val == null){
                        values.add(null);
                        recordAttributeSizes.add(1);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    if(isNumeric(val)){
                        if(val.contains(".")){
                            System.out.println("EXPECTED INTEGER, GOT DOUBLE");
                            return null;
                        }
                        else{
                            values.add(Double.parseDouble(val));
                            recordAttributeSizes.add(Double.SIZE/8);
                            if(i == tableAttributes.size()-1){
                                allRecordsAttributeSizes.add(recordAttributeSizes);
                            }
                            continue;
                        }
                    }
                    else{
                        System.out.println("EXPECTED DOUBLE, GOT NOT DOUBLE ERROR");
                        return null;
                    }
                }
                else if(type.equals("integer")){
                    if(val == null){
                        values.add(null);
                        recordAttributeSizes.add(1);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    if(isNumeric(val)){
                        if(val.contains(".")){
                            System.out.println("EXPECTED INTEGER, GOT DOUBLE");
                            return null;
                        }
                        else{
                            values.add(Integer.parseInt(val));
                            recordAttributeSizes.add(Integer.SIZE/8);
                            if(i == tableAttributes.size()-1){
                                allRecordsAttributeSizes.add(recordAttributeSizes);
                            }
                            continue;
                        }
                    }
                    else{
                        System.out.println("EXPECTED INTEGER, GOT NOT INTEGER ERROR");
                        return null;
                    }
                }
                else if(type.equals("boolean")){
                    if(val == null){
                        values.add(null);
                        recordAttributeSizes.add(1);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    if(val.equals("true")){
                        values.add(Boolean.parseBoolean(val));
                        recordAttributeSizes.add(1);
                        if(i == tableAttributes.size()-1){
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    else if(val.equals("false")) {
                        values.add(Boolean.parseBoolean(val));
                        recordAttributeSizes.add(1);
                        if (i == tableAttributes.size() - 1) {
                            allRecordsAttributeSizes.add(recordAttributeSizes);
                        }
                        continue;
                    }
                    else{
                        return null;
                    }
                }
                else{
                    System.out.println("INVALID TYPE ERROR");
                    return null;
                }
            }
        }
        toReturn[0] = values;
        toReturn[1] = allRecordsAttributeSizes;
        return toReturn;
    }

    /**
     * checks if the passed in string is either int or double.
     * @param str the string being checked.
     * @return true if the string is numeric, false otherwise.
     */
    private Boolean isNumeric(String str){
        try {
            if(str.contains(".")){
                double val = Double.parseDouble(str);
            }
            else{
                int val = Integer.parseInt(str);
            }

        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    /**
     * Takes in a table and page number, gets the number of records in that
     * page.
     * @param tableName the tabble whose page's number of records is being
     *                  gotten from.
     * @param pageNum the page number whose number of records is being gotten
     *                from.
     * @return The number of records in the passed page.
     */
    private int getNumRecords(String tableName, int pageNum){
        byte[] first_page = bm.getPage(tableName, pageNum); // get page

        byte[] bytesNumRecs = new byte[Integer.SIZE]; // byte array for number of records

        for (int j = 0; j < Integer.SIZE; j++) {
            bytesNumRecs[j] = first_page[j]; // set each byte of numrecs to byte of page
        }
        int numRecs = 0;
        for (byte b : bytesNumRecs) {
            numRecs = (numRecs << 8) + (b & 0xFF); //convert bytes of num recs to actual value.
        }
        return numRecs;
    }

    /**
     * Converts the passed in tuples to a byte array.
     *
     * @param result
     * @param tuples the arraylist of tuples.
     * @param values
     * @param table
     * @return
     */
    private static ArrayList<byte[]> convertToBytes(ArrayList<ArrayList<String>> tuples,
                                                    ArrayList<ArrayList<Integer>> sizes,
                                                    ArrayList<Object> values,
                                                    Schema table){
        ArrayList<byte[]> byteArrays = new ArrayList<>();
        int intSize = Integer.SIZE/8;
        for(int j = 0; j < tuples.size(); j++){ //for each tuple in tuples

            ArrayList<String> tuple = tuples.get(j);                    // tuple i
            ArrayList<Integer> tupleSizes = sizes.get(j);               // tuple i sizes
            int sizeOfPointers = tupleSizes.size() * 2 * Integer.SIZE;  // size of all pointers
            int nullBitMapLength = tuple.size();         // size of null bit map
            int sizeOfNonData = nullBitMapLength + sizeOfPointers;      // adds up size of non data
            int totalSize = sizeOfNonData;

            for(int i = 0; i < tupleSizes.size(); i++){
                totalSize += tupleSizes.get(i);                        // total size = nondata + data
            }

            byte[] bytes = new byte[totalSize];                        //

            int dataLoc = sizeOfNonData;
            int location = 0;
            int prevSizes = 0;
            int curLocation = 0;
            ArrayList<Integer> locations = new ArrayList<>();
            for(int i = 0; i < tuple.size(); i++) { // for each attribute in each tuple

                int size = tupleSizes.get(i);       // gets the size of the attribute
                if(i == 0){
                    location = dataLoc;
                }
                else{
                    location += prevSizes;
                }
                prevSizes += size;
                locations.add(location);
                for(int k = 0; k < intSize; k++){
                    bytes[curLocation + k] = (byte) (location >>> ((intSize-1)*8 - (8 * k))); //add attribute location
                    bytes[curLocation + k + intSize] = (byte) (size >>> ((intSize-1)*8 - (8 * k))); //add attribute size
                }
                curLocation += intSize * 2;

            }
            for(int i = 0; i < nullBitMapLength; i++){      // add null bitMap
                curLocation += i;
                if(tuple.get(i) == null){
                    bytes[curLocation] = 1;
                }
                else{
                    bytes[curLocation] = 0;
                }
            }
            ArrayList<Attribute> attributes = table.getAttributes();
            for(int i = 0; i < values.size(); i++){        // add values
                String value = attributes.get(i).getType();
                Object obj = values.get(i);
                if(value.equals("integer")){
                    int num = (int) obj;
                    for(int k = 0; k < intSize; k++){
                        bytes[curLocation + k] = (byte) (num >>> ((intSize-1)*8 - (8 * k)));
                    }
                    curLocation += intSize;

                }
                else if(value.equals("double")){
                    double valDouble = (double) obj;
                    long longValue = Double.doubleToLongBits(valDouble);
                    byte[] byteArray = ByteBuffer.allocate(Long.BYTES).putLong(longValue).array();
                    for(int k = 0; k < byteArray.length; k++){
                        bytes[curLocation + k] = byteArray[k];
                        curLocation += k;
                    }
                }
                else if(value.equals("boolean")){
                    boolean valBool = (boolean) obj;
                    if(valBool){
                        bytes[curLocation + 1] = 1;
                        curLocation++;
                    }
                    else{
                        bytes[curLocation + 1] = 1;
                        curLocation++;
                    }

                }
                else if(value.equals("char")){
                    String valBool = (String) obj;
                    byte[] byteArray = valBool.getBytes();
                    for(int k = 0; k < byteArray.length; k++){
                        bytes[curLocation + k] = byteArray[k];
                        curLocation += k;
                    }

                }
                else if(value.equals("varchar")){
                    String valDouble = (String) obj;
                    byte[] byteArray = valDouble.getBytes();
                    for(int k = 0; k < byteArray.length; k++){
                        bytes[curLocation + k] = byteArray[k];
                        curLocation += k;
                    }
                }
            }
            byteArrays.add(bytes);

        }
        return byteArrays;
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
