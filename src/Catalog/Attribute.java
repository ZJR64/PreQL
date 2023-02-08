package src.Catalog;

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
    public Attribute (String input) {
        String[] filtered = input.split(" ");
        //type is always first
        this.type = filtered[0];
        //size is next
        this.size = Integer.parseInt(filtered[1]);
        //then name
        this.name = filtered[2];
        //the rest are descriptors
        descriptors = new ArrayList<String>();
        for (int i = 3; i < filtered.length; i++) {
            descriptors.add(filtered[i]);
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
    public String writeable() {
        String output = type + " " + size + " " + name;
        if (descriptors != null) {
            for (String descriptor: descriptors) {
                output += " " + descriptor;
            }
        }
        return output;
    }

}
