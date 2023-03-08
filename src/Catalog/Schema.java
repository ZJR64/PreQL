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

    String seperator = "!@#%&_";
    private String name;
    private int pages;
    private int records;
    private ArrayList<Attribute> attributes;
    private ArrayList<Integer> pageOrder;
    private ArrayList<Integer> openPages;

    /**
     * Constructor for a new Schema object
     *
     * @param name the name of the table.
     * @param attributes the list of non-key attributes for the table.
     */
    public Schema (String name, ArrayList<Attribute> attributes) {
        this.name = name;
        this.attributes = attributes;
        this.pageOrder = new ArrayList<Integer>();
        this.openPages = new ArrayList<Integer>();
        this.pages = 0;
        this.records = 0;
    }

    /**
     * Constructor for the Schema object when read from file.
     *
     * @param input the String containing the schema.
     */
    public Schema (String input) {
        String[] filtered = input.split(seperator);
        //name is always first
        this.name = filtered[0];
        //next is pages
        this.pages = Integer.parseInt(filtered[2]);
        //then records
        this.records = Integer.parseInt(filtered[3]);
        //get pageOrder
        this.pageOrder = new ArrayList<Integer>();
        for (int i = 4; i < pages + 4; i++) {
            pageOrder.add(Integer.parseInt(filtered[i]));
        }
        //get openPages
        this.openPages = new ArrayList<Integer>();
        for (int i = pages + 4; i < pages*2 + 4; i++) {
            openPages.add(Integer.parseInt(filtered[i]));
        }
        //get rest of attributes
        this.attributes = new ArrayList<Attribute>();
        for (int i = 4 + pages; i < filtered.length; i++) {
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
        String output = name + seperator + seperator + pages + seperator + records;
        for (int page : pageOrder) {
            output += seperator + page;
        }
        for (int page : openPages) {
            output += seperator + page;
        }
        for (Attribute a: attributes) {
            output += seperator + a.writeable();
        }
        return output;
    }

    /**
     * getter method for name of the table.
     *
     * @return name of the schema.
     */
    public String getName() {return name;}

    /**
     * getter method for name of the file the table is stored on.
     *
     * @return path to the table.
     */
    public String getFileName() {return name + ".tbl";}

    /**
     * getter method for number of pages the table occupies.
     *
     * @return number of pages.
     */
    public int getPages() {return pages;}

    /**
     * Adds a page to the pageOrder. If this is the first page being
     * added, then before does not matter.
     *
     * @param before the page immediately before the one added.
     * @param page the page number being added.
     */
    public void addPage(int before, int page) {
        //check if first page added
        if (pages == 0) {
            pageOrder.add(page);
        }
        else {
            //add page to pageOrder
            pageOrder.add(pageOrder.indexOf(before) + 1, page);
        }
        //check if in openPages, and if so, then remove
        if (openPages.indexOf(page) != -1) {
            openPages.remove(openPages.indexOf(page));
        }
        //increment pages
        this.pages++;
    }

    /**
     * Subtracts one to the number of pages the table takes up
     *
     * @param page the page to be removed.
     */
    public void subPage(int page) {
        //remove from pageOrder then add to openPages
        pageOrder.remove(pageOrder.indexOf(page));
        openPages.add(page);
        //increment pages
        this.pages--;
    }

    /**
     * getter method for page order.
     *
     * @return ArrayList of page order.
     */
    public ArrayList<Integer> getPageOrder() {
        return pageOrder;
    }

    /**
     * getter method for the list of open pages.
     *
     * @return ArrayList of open pages.
     */
    public ArrayList<Integer> getOpenPages() {
        return openPages;
    }

    /**
     * getter method for the number of records the table has.
     *
     * @return number of records.
     */
    public int getRecords() {return records;}

    /**
     * Adds one to the number of records the table has.
     */
    public void addRecord() {this.records++;}

    /**
     * Subtracts one to the number of records the table has.
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
