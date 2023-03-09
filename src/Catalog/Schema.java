package src.Catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

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
        this.pageOrder = new ArrayList<>();
        this.openPages = new ArrayList<>();
        this.pages = 0;
        this.records = 0;
    }

    /**
     * Constructor for the Schema object when read from file.
     *
     * @param buffer the buffer containing the schema.
     */
    public Schema (ByteBuffer buffer) {
        //get name
        int nameSize = buffer.getInt();
        byte[] nameArray = new byte[nameSize];
        buffer.get(nameArray);
        this.name = new String(nameArray);

        //get pages
        this.pages = buffer.getInt();

        //get records
        this.records = buffer.getInt();

        //get pageOrder
        this.pageOrder = new ArrayList<>();
        while(pageOrder.size() < this.pages) {
            pageOrder.add(buffer.getInt());
        }

        //get number of open pages
        int numOpen = buffer.getInt();
        //get openPages
        this.openPages = new ArrayList<>();
        while(openPages.size() < numOpen) {
            openPages.add(buffer.getInt());
        }

        //get number of attributes
        int numAttributes = buffer.getInt();
        //get rest of attributes
        this.attributes = new ArrayList<>();
        while(attributes.size() < numAttributes) {
            attributes.add(new Attribute(buffer));
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
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getSchemaByteSize()]);

        //set name
        buffer.putInt(this.name.length());
        buffer.put(this.name.getBytes());

        //set pages
        buffer.putInt(this.pages);

        //set records
        buffer.putInt(this.records);

        //set pageOrder
        for (int page : pageOrder) {
            buffer.putInt(page);
        }

        //set number of open pages
        buffer.putInt(openPages.size());
        //set openPages
        for (int page : openPages) {
            buffer.putInt(page);
        }

        //set number of attributes
        buffer.putInt(attributes.size());
        //set rest of attributes
        for (Attribute attribute: attributes) {
            byte[] attributeBytes = attribute.toBytes();
            buffer.put(attributeBytes);
        }

        return buffer.array();
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
        //sort openPages so earlier gaps are filled first
        Collections.sort(openPages);
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

    /**
     * get the size of a byte array for the schema
     *
     * @return the size of a byte array
     */
    public int getSchemaByteSize() {
        int arraySize = 0;

        //add an integer and string for name
        arraySize += Integer.SIZE/Byte.SIZE;
        arraySize += this.name.length();

        //add integer for pages
        arraySize += Integer.SIZE/Byte.SIZE;

        //add integer for records
        arraySize += Integer.SIZE/Byte.SIZE;

        //add integer for each page
        arraySize += (Integer.SIZE/Byte.SIZE) * pageOrder.size();

        //add integer for num of open pages
        arraySize += Integer.SIZE/Byte.SIZE;
        //add integer for each open page
        arraySize += (Integer.SIZE/Byte.SIZE) * openPages.size();

        //add integer for number of attributes
        arraySize += Integer.SIZE/Byte.SIZE;
        //add the size of each attribute
        for (Attribute attribute : attributes) {
            arraySize += attribute.getAttributeByteSize();
        }

        //return array size
        return arraySize;
    }

}
