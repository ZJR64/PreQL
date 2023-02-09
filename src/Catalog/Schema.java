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

    /**
     * getter method for name of the table.
     *
     * @return name of the schema.
     */
    public String getName() {return name;}

    /**
     * getter method for number of pages the table occupies.
     *
     * @return number of pages.
     */
    public int getPages() {return pages;}

    /**
     * Adds one to the number of pages the table takes up
     */
    public void addPage() {this.pages++;}

    /**
     * Subtracts one to the number of pages the table takes up
     */
    public void subPage() {this.pages--;}

    /**
     * getter method for the number of records the table has.
     *
     * @return number of records.
     */
    public int getRecords() {return records;}

    /**
     * Adds one to the number of pages the table takes up
     */
    public void addRecord() {this.records++;}

    /**
     * Subtracts one to the number of pages the table takes up
     */
    public void subRecord() {this.records--;}

    /**
     * Searches through the collection of attributes
     * and searches for one with the given name.
     * This method is case-sensitive.
     *
     * @return the attribute, or null if not found.
     */
    public Attribute getAttribute(String name) {
        for (Attribute attribute: attributes) {
            if (attribute.getName().equals(name)) {
                return attribute;
            }
        }
        return null;
    }

    /**
     * getter method for the collection of attributes.
     *
     * @return the arraylist of schemas.
     */
    public ArrayList<Attribute> getAttributes() {
        return attributes;
    }

    /**
     * Finds the primaryKey from the list of attributes and returns it.
     *
     * @return the attribute that is the primary key, or null if not found.
     */
    public Attribute getKey()  {
        for (Attribute attribute: attributes) {
            for (String descriptor: attribute.getDescriptors()) {
                if (descriptor.equalsIgnoreCase("primarykey")) {
                    return attribute;
                }
            }
        }
        return null;
    }

}
