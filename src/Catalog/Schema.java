package src.Catalog;

import java.util.ArrayList;

public class Schema {

    private String name;
    private Attribute key;
    private Attribute[] attributes;

    /**
     * Constructor for the Schema object
     *
     * @param name the name of the table.
     * @param key the key attribute for the table.
     * @param attributes the list of non-key attributes for the table.
     */
    public Schema (String name, Attribute key, Attribute[] attributes) {
        this.name = name;
        this.key = key;
        this.attributes = attributes;
    }

    /**
     * Constructor for the Schema object when read from file.
     *
     * @param input the byte array containing the schema.
     */
    public Schema (String input) {
        String[] filtered = input.split(" ");
        //name is always first
        this.name = filtered[0];
        //key always follows
        this.key = new Attribute(filtered[1], filtered[2]);
        //get rest of attributes
        ArrayList<Attribute> temp = new ArrayList<Attribute>();
        this.attributes = new Attribute[(filtered.length - 3)/2];
        for (int i = 3; i < filtered.length; i+=2) {
            attributes[(i - 3)/2] = new Attribute(filtered[i], filtered[i+1]);
        }
    }
    /**
     * toString method for the schema that is used when the schema needs to be displayed.
     *
     * @return the schema in string form.
     */
    @Override
    public String toString() {
        String output = name + "\n";
        output += "\t" + key.toString() + "\n";
        for (Attribute a: attributes) {
            output += "\t" + a.toString() + "\n";
        }
        return output;
    }

    /**
     * method for the schema that is used when the schema needs to be
     * written to a file in binary.
     *
     * @return the schema in writable form.
     */
    public String writeable() {
        String output = name + " ";
        output += key.toString();
        for (Attribute a: attributes) {
            output += " " + a.toString();
        }
        output += ";";
        return output;
    }


}
