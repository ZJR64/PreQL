package src.StorageManager;

import src.Catalog.Attribute;
import src.Catalog.Schema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLOutput;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Record {
    private Map<String, Object> attributes;
    private int size;
    private Schema schema;

    /**
     * Constructor for records when we have a byte array.
     *
     * @param schema the schema the record uses.
     * @param byteBuffer the byteBuffer that composes the record.
     */
    public Record (Schema schema, ByteBuffer byteBuffer) {
        this.schema = schema;
        //put data in the map
        this.attributes = makeSense(byteBuffer);
        this.size = calcSize();
    }

    /**
     * Constructor for records when we have the attribute map.
     *
     * @param schema the schema the record uses.
     * @param attributes the map of attributes with names.
     */
    public Record (Schema schema, Map<String, Object> attributes) {
        this.schema = schema;
        this.attributes = attributes;
        this.size = calcSize();
    }

    /**
     * getter method for the attributes in the record
     *
     * @return a map with names of attribute and value.
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void addAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    public void removeAttribute(String name) {
        attributes.remove(name);
    }

    /**
     * Sets the attributes in the record.
     *
     * @param attributes the attributes to set.
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Object getKey() {
        Attribute keyAttribute = schema.getKey();
        return attributes.get(keyAttribute.getName());
    }

    /**
     * gets the type of the primaryKey.
     * @return A string containing the type of the primarykey.
     */
    public String getKeyType(){
        Attribute keyAttribute = schema.getKey();
        return keyAttribute.getType();
    }

    /**
     * Gets the value of an attribute.
     *
     * @param name the name of the attribute.
     * @return an object that corresponds to the attribute name.
     */
    public Object getValue(String name) {
        return attributes.get(name);
    }

    /**
     * Sets the value of an attribute.
     *
     * @param name the name of the attribute to set.
     * @param value the value of the attribute being set.
     */
    public void setValue(String name, Object value) {
        attributes.put(name, value);
    }

    /**
     * Gets the size of the record.
     *
     * @return the size of the record.
     */
    public int getSize() {
        return size;
    }

    public Schema getSchema() { return  schema;}

    /**
     * Gets the byte representation of the record.
     *
     * @return the byte array of the record;
     */
    public byte[] getBytes() {
        return makeNonsense();
    }

    /**
     * Gets the key of the record.
     *
     * @return the key value of the record.
     */
    public Object getPrimaryKey() {
        String keyName = schema.getKey().getName();
        return attributes.get(keyName);
    }

    /**
     * Takes a byte array and puts the attributes into the map.
     *
     * @param buffer the buffer to be looked through.
     */
    private Map<String, Object> makeSense(ByteBuffer buffer) {
        //setup
        Map<String, Object> attributeMap = new HashMap<>();

        //read each attribute
        int attributeIndex = 0;
        for (Attribute attribute : schema.getAttributes()) {
            //check if null
            if (buffer.get() == 1) {
                attributeMap.put(attribute.getName(), null);
                continue;
            }

            //get attribute length
            int length = buffer.getInt();

            //check to see what type attribute is
            if (attribute.getType().contains("char")) {
                //varchar or char
                byte[] stringBytes = new byte[length];
                buffer.get(stringBytes);
                attributeMap.put(attribute.getName(), new String(stringBytes));
            }
            else if (attribute.getType().equalsIgnoreCase("integer")) {
                //integer
                attributeMap.put(attribute.getName(), buffer.getInt());
            }
            else if (attribute.getType().equalsIgnoreCase("boolean")) {
                //boolean
                byte value = buffer.get();
                attributeMap.put(attribute.getName(), value == 1);
            }
            else {
                //double
                attributeMap.put(attribute.getName(), buffer.getDouble());
            }
        }

        return attributeMap;
    }

    /**
     * convert the map into a byte array.
     *
     * @return the byte array representation of the record.
     */
    private byte[] makeNonsense() {
        //setup
        ByteBuffer buffer = ByteBuffer.wrap(new byte[calcSize()]);

        //go through each attribute
        for (Attribute attribute : schema.getAttributes()) {
            Object value = attributes.get(attribute.getName());

            //check if null
            byte nullValue = 0;
            if (attributes.get(attribute.getName()) == null) {
                nullValue = 1;
                buffer.put(nullValue);
                continue;
            }
            buffer.put(nullValue);


            //look to see what type of attribute it is
            if (attribute.getType().contains("char")) {
                //varchar or char
                byte[] stringBytes = ((String) value).getBytes();
                buffer.putInt(stringBytes.length);
                buffer.put(stringBytes);
            } else if (attribute.getType().equalsIgnoreCase("integer")) {
                //integer
                buffer.putInt(Integer.BYTES);
                buffer.putInt((Integer)value);
            }
            else if (attribute.getType().equalsIgnoreCase("boolean")) {
                //boolean
                buffer.putInt(1);
                if ((boolean) value) {
                    buffer.put((byte) 1);
                }
                else {
                    buffer.put((byte) 0);
                }
            }
            else {
                //must be double
                buffer.putInt(Double.BYTES);
                buffer.putDouble((Double)value);
            }
        }
        return buffer.array();
    }

    @Override
    public boolean equals(Object primaryKey) {
        try {
            String primaryAttribute = this.schema.getKey().getType();
            //look to see what type of attribute it is
            if (primaryAttribute.contains("char")) {
                //string
                String primaryKeyOther = (String) primaryKey;
                String primaryKeyNative = (String) this.getPrimaryKey();

                //compare
                if (primaryKeyNative.equals(primaryKeyOther)) {
                    return true;
                }
            }
            else if (primaryAttribute.equalsIgnoreCase("integer")) {
                //integer
                Integer primaryKeyOther = (Integer) primaryKey;
                Integer primaryKeyNative = (Integer) this.getPrimaryKey();

                //compare
                if (primaryKeyNative.equals(primaryKeyOther)) {
                    return true;
                }
            }
            else if (primaryAttribute.equalsIgnoreCase("boolean")) {
                //boolean, technincally it could be a key
                boolean primaryKeyOther = (boolean) primaryKey;
                boolean primaryKeyNative = (boolean) this.getPrimaryKey();

                //compare
                if (primaryKeyNative == primaryKeyOther) {
                    return true;
                }
            }
            else {
                //must be double
                double primaryKeyOther = (double) primaryKey;
                double primaryKeyNative = (double) this.getPrimaryKey();

                //compare
                if (primaryKeyNative == primaryKeyOther) {
                    return true;
                }
            }
        }
        catch (Exception e) {
            System.err.println(e);
        }
        return false;
    }

    public boolean greaterThan(Object primaryKey) {
        try {
            String primaryAttribute = this.schema.getKey().getType();
            //look to see what type of attribute it is
            if (primaryAttribute.contains("char")) {
                //string
                String primaryKeyOther = (String) primaryKey;
                String primaryKeyNative = (String) this.getPrimaryKey();

                //compare
                if (primaryKeyNative.compareTo(primaryKeyOther) > 0) {
                    return true;
                }
            }
            else if (primaryAttribute.equalsIgnoreCase("integer")) {
                //integer
                int primaryKeyOther = (int) primaryKey;
                int primaryKeyNative = (int) this.getPrimaryKey();

                //compare
                if (primaryKeyNative > primaryKeyOther) {
                    return true;
                }
            }
            else if (primaryAttribute.equalsIgnoreCase("boolean")) {
                //boolean, technincally it could be a key
                boolean primaryKeyOther = (boolean) primaryKey;
                boolean primaryKeyNative = (boolean) this.getPrimaryKey();

                //compare
                if (primaryKeyNative != primaryKeyOther) {
                    return true;
                }
            }
            else {
                //must be double
                double primaryKeyOther = (double) primaryKey;
                double primaryKeyNative = (double) this.getPrimaryKey();

                //compare
                if (primaryKeyNative > primaryKeyOther) {
                    return true;
                }
            }
        }
        catch (Exception e) {}
        return false;
    }

    /**
     * calculate the size of the record
     */
    private int calcSize() {
        int recordSize = 0;

        //count each attribute
        for (Attribute attribute : schema.getAttributes()) {
            //add 1 for null indicator
            recordSize += 1;
            //do not count rest of null values
            if (attributes.get(attribute.getName()) == null) {
                continue;
            }

            //add integer for attribute length
            recordSize += Integer.BYTES;

            //look to see what type of attribute it is
            if (attribute.getType().contains("char")) {
                //varchar or char
                recordSize += ((String) attributes.get(attribute.getName())).getBytes().length;
            } else if (attribute.getType().equalsIgnoreCase("integer")) {
                //integer
                recordSize += Integer.BYTES;
            }
            else if (attribute.getType().equalsIgnoreCase("boolean")) {
                //boolean is always 1 byte
                recordSize += 1;
            }
            else {
                //must be double
                recordSize += Double.BYTES;
            }
        }

        //return size
        return recordSize;
    }
}