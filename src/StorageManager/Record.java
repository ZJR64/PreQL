package src.StorageManager;

import src.Catalog.Attribute;
import src.Catalog.Schema;

import java.nio.ByteBuffer;
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
     * @param data the byte array that composes the record.
     */
    public Record (Schema schema, byte[] data) {
        this.schema = schema;
        this.size = data.length;
        //put data in the map
        this.attributes = makeSense(data);
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

    /**
     * Sets the attributes in the record.
     *
     * @param attributes the attributes to set.
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
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

    /**
     * Gets the byte representation of the record.
     *
     * @return the byte array of the record;
     */
    public byte[] getBytes() {
        return makeNonsense();
    }

    /**
     * Takes a byte array and puts the attributes into the map.
     *
     * @param data the byte array to be looked through.
     */
    private Map<String, Object> makeSense(byte[] data) {
        //setup
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Map<String, Object> attributeMap = new HashMap<>();

        //get nulls
        byte[] nullBytes = new byte[(int) (schema.getAttributes().size() + Byte.SIZE - 1) / Byte.SIZE];
        buffer.get(nullBytes);
        BitSet nullBitMap = BitSet.valueOf(nullBytes);

        //read each attribute
        int attributeIndex = 0;
        for (Attribute attribute : schema.getAttributes()) {
            //check if null
            if (nullBitMap.get(attributeIndex)) {
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
        ByteBuffer buffer = ByteBuffer.wrap(new byte[this.size]);

        //create null bitmap
        BitSet nullBitMap = new BitSet(schema.getAttributes().size());
        int attributeIndex = 0;
        for (Attribute attribute : schema.getAttributes()) {
            //check if null
            if (attributes.get(attribute.getName()) == null) {
                //set null bit
                nullBitMap.set(attributeIndex);
                continue;
            }
        }
        buffer.put(nullBitMap.toByteArray());

        //go through each attribute
        for (Attribute attribute : schema.getAttributes()) {
            Object value = attributes.get(attribute.getName());

            //look to see what type of attribute it is
            if (attribute.getType().contains("char")) {
                //varchar or char
                byte[] stringBytes = ((String) value).getBytes();
                buffer.putInt(stringBytes.length);
                buffer.put(stringBytes);
            } else if (attribute.getType().equalsIgnoreCase("integer")) {
                //integer
                buffer.putInt(Integer.SIZE/Byte.SIZE);
                buffer.putInt((Integer)value);
            }
            else if (attribute.getType().equalsIgnoreCase("boolean")) {
                //boolean
                buffer.putInt(1);
                if ((boolean) value) {
                    buffer.putInt(1);
                }
                else {
                    buffer.putInt(0);
                }
            }
            else {
                //must be double
                buffer.putInt(Double.SIZE/Byte.SIZE);
                buffer.putDouble((Double)value);
            }
        }
        return buffer.array();
    }

    /**
     * calculate the size of the record
     */
    /**
     * finds the size of the byte array required for a record.
     *
     * @param record the map of name and value.
     * @return the potential size of a byte array.
     */
    private int calcSize() {
        int recordSize = 0;

        //count null bitmap
        recordSize = (int) (schema.getAttributes().size() + Byte.SIZE - 1) / Byte.SIZE;

        //count each attribute
        for (Attribute attribute : schema.getAttributes()) {
            //do not count null values
            if (attributes.get(attribute.getName()) == null) {
                continue;
            }

            //add integer for attribute length
            recordSize += Integer.SIZE/Byte.SIZE;

            //look to see what type of attribute it is
            if (attribute.getType().contains("char")) {
                //varchar or char
                recordSize += ((String) attributes.get(attribute)).getBytes().length;
            } else if (attribute.getType().equalsIgnoreCase("integer")) {
                //integer
                recordSize += Integer.SIZE/Byte.SIZE;
            }
            else if (attribute.getType().equalsIgnoreCase("boolean")) {
                //boolean is always 1 byte
                recordSize += 1;
            }
            else {
                //must be double
                recordSize += Double.SIZE/Byte.SIZE;
            }
        }

        //return size
        return recordSize;
    }
}