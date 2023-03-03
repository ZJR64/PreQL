package src.StorageManager;

import src.Catalog.Schema;
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
        //put data in the map
        makeSense(data);
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
     * Gets the value of an attribute.
     *
     * @param name the name of the attribute.
     * @return an object that corresponds to the attribute name.
     */
    public Object getValue(String name) {
        return attributes.get(name);
    }

    /**
     * Takes a byte array and puts the attributes into the map.
     *
     * @param data the byte array to be looked through.
     */
    private void makeSense(byte[] data) {

    }

    /**
     * convert the map into a byte array.
     *
     * @return the byte array representation of the record.
     */
    private byte[] makeNonsense() {
        return null;
    }

    /**
     * calculate the size of the record
     */
    private int calcSize() {
        //TODO
        return -1;
    }
}