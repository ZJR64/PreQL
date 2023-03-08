package src.Catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * The class for Attributes. This class is used to stare basic information about each
 * attribute added to the database, including the name, variable type, size, and
 * relations.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Attribute {

    private String type;
    private int size;
    private ArrayList<String> descriptors;
    private String name;

    /**
     * Constructor for a new Attribute object
     *
     * @param type the typ of attribute this represents.
     * @param size the amount of bytes the attribute takes up.
     * @param name the name of the attribute.
     * @param descriptors the descriptors of the parameter, can include anything.
     */
    public Attribute (String type, int size, String name, ArrayList<String> descriptors) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.descriptors = descriptors;
    }

    /**
     * Constructor for the Attribute object when read from file.
     *
     * @param input the String containing the attribute.
     */
    public Attribute (ByteBuffer buffer) {
        //get type
        int typeSize = buffer.getInt();
        byte[] typeArray = new byte[typeSize];
        buffer.get(typeArray);
        this.type = new String(typeArray);

        //get size
        this.size = buffer.getInt();

        //get name
        int nameSize = buffer.getInt();
        byte[] nameArray = new byte[nameSize];
        buffer.get(nameArray);
        this.name = new String(nameArray);

        //get descriptors
        int numDescriptors = buffer.getInt();
        descriptors = new ArrayList<String>();
        for (int i = 0; i < numDescriptors; i++) {
            int descriptorSize = buffer.getInt();
            byte[] descriptorArray = new byte[descriptorSize];
            buffer.get(descriptorArray);
            descriptors.add(new String(descriptorArray));
        }
    }

    /**
     * toString method for the schema that is used when the schema needs to be displayed.
     *
     * @return the schema in string form.
     */
    @Override
    public String toString() {
        String output = type;
        if (type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
            output += "(" + size + ")";
        }
        output += " " + name;
        if (descriptors != null) {
            for (String descriptor: descriptors) {
                output += ", " + descriptor;
            }
        }
        return output;
    }

    /**
     * method for the attribute that is used when the attribute needs to be
     * written to a file in binary.
     *
     * @return the attribute in writable form.
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getAttributeByteSize()]);

        //set type
        buffer.putInt(this.type.length());
        buffer.put(this.type.getBytes());

        //set size
        buffer.putInt(this.size);

        //set name
        buffer.putInt(this.name.length());
        buffer.put(this.name.getBytes());

        //set numDescriptors
        buffer.putInt(descriptors.size());

        //set each descriptor
        for (String descriptor : descriptors) {
            buffer.putInt(descriptor.length());
            buffer.put(descriptor.getBytes());
        }

        return buffer.array();
    }

    /**
     * getter method for size of the attribute
     *
     * @return size of the attribute.
     */
    public int getSize() {return size;}

    /**
     * getter method for name of the attribute
     *
     * @return name of the attribute.
     */
    public String getName() {return name;}

    /**
     * getter method for type of the attribute
     *
     * @return tyoe of the attribute.
     */
    public String getType() {return type;}

    /**
     * getter method for descriptors of the attribute
     *
     * @return an arraylist of the descriptors
     */
    public ArrayList<String> getDescriptors() {return descriptors;}

    /**
     * get the size of a byte array for the attribute
     *
     * @return the size of a byte array
     */
    public int getAttributeByteSize() {
        int arraySize = 0;

        //add an integer and string for type
        arraySize += Integer.SIZE/Byte.SIZE;
        arraySize += this.type.length();

        //size is an int
        arraySize += Integer.SIZE/Byte.SIZE;

        //add an integer and string for name
        arraySize += Integer.SIZE/Byte.SIZE;
        arraySize += this.name.length();

        //num descriptors will be an integer
        arraySize += Integer.SIZE/Byte.SIZE;

        //add each descriptor
        for (String descriptor : descriptors) {
            arraySize += Integer.SIZE/Byte.SIZE;
            arraySize += descriptor.length();
        }

        //return size
        return arraySize;
    }
}
