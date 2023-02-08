package src.Catalog;

import java.util.ArrayList;

/**
 * This class is used mostly to organise and store the attribute objects. It
 * is also equipped to convert the attributes to strings and a writable form
 * when required.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Schema {

    private String name;
    private int pages;
    private int records;
    private ArrayList<Attribute> attributes;

    /**
     * Constructor for a new Schema object
     *
     * @param name the name of the table.
     * @param key the key attribute for the table.
     * @param attributes the list of non-key attributes for the table.
     */
    public Schema (String name, Attribute key, ArrayList<Attribute> attributes) {
        this.name = name;
        this.attributes = attributes;
        this.pages = 0;
        this.records = 0;
    }

    /**
     * Constructor for the Schema object when read from file.
     *
     * @param input the String containing the schema.
     */
    public Schema (String input) {
        String[] filtered = input.split("~");
        //name is always first
        this.name = filtered[0];
        //next is pages
        this.pages = Integer.parseInt(filtered[1]);
        //then records
        this.records = Integer.parseInt(filtered[2]);
        //get rest of attributes
        this.attributes = new ArrayList<Attribute>();
        for (int i = 3; i < filtered.length; i++) {
            attributes.add(new Attribute(filtered[i]));
        }
    }

    /**
     * toString method for the schema that is used when the schema needs to be displayed.
     *
     * @return the schema in string form.
     */
    @Override
    public String toString() {
        String output = name;
        for (Attribute a: attributes) {
            output += "\n\t" + a.toString();
        }
        output += "\nPages: " + pages;
        output += "\nRecords: " + records;
        return output;
    }

    /**
     * method for the schema that is used when the schema needs to be
     * written to a file in binary.
     *
     * @return the schema in writable form.
     */
    public String writeable() {
        String output = name + "~" + pages + "~" + records;
        for (Attribute a: attributes) {
            output += "~" + a.writeable();
        }
        output += ";";
        return output;
    }


}
