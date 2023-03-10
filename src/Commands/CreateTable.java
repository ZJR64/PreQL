package src.Commands;

import src.Catalog.Attribute;
import src.StorageManager.StorageManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 * The class for the Create Table Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 * @contributor Nicholas Lewandowski njl4420@rit.edu
 */
public class CreateTable extends Command{

    // Map of the name of the attribute eot the type stored as a String
    //private Map<Strin, String> attributesNameToType;
    // name of the primary key
    private String primarykeyName;
    // name of the table to create
    private String name;

    private ArrayList<Attribute> ats;

    private StorageManager sm;

    // default constructor
    public CreateTable(String input, StorageManager sm){
        super(input);
        this.sm = sm;
    }

    /**
     * parses the command and stores the table name, attributes to attribute type, primary key name,
     * and if the parse was successful.
     */
    @Override
    public void parse() {
        try {
            //this.attributesNameToType = new HashMap<>();
            this.ats = new ArrayList<>();
            boolean hasPrimary = false;
            // split on first occurrence of (
            //["create table <name>", attributes,...]
            String[] tokens = input.split("\\(", 2);
            // get the name
            this.name = tokens[0].split("(?i)table")[1].strip();

            // empty attributes
            if (tokens[1].equals(");")) {
                this.success = true;
            } else {
                // parse attributes
                String[] attributes = tokens[1].strip().split(",");
                for (String a : attributes) {
                    a = a.replace(");", "").strip();
                    a = a.replace(";", "").strip();
                    boolean currentIsPrimary = false;
                    boolean notnull = false;
                    boolean unique = false;

                    String[] splitAtts = a.split(" ");
                    // if primary key, store that it's the primary key
                    if (splitAtts.length > 2 && splitAtts[2].replaceAll("\\)*", "").equalsIgnoreCase("primarykey")) {
                        if (hasPrimary){
                            System.out.println(input + " has multiple primary keys. Cannot create table.");
                            this.success = false;
                            break;
                        }
                        this.primarykeyName = splitAtts[0];
                        hasPrimary = true;
                        currentIsPrimary = true;
                    }
                    // check for notnull and unique constraint
                    else if (splitAtts.length > 2){
                        if (splitAtts[2].strip().replace(")","").replace(";", "").equalsIgnoreCase("notnull")){
                            notnull = true;
                        }
                        else if (splitAtts[2].strip().replace(")","").replace(";", "").equalsIgnoreCase("unique")){
                            unique = true;
                        }
                    }
                    String attributeType = splitAtts[1];
                    // if the string contains only closing parenthesis, remove it
                    // keeps both parenthesis if both opening and closing are there
                    if (attributeType.contains(")") && !attributeType.contains("(")){
                        attributeType = attributeType.replace(")", "");
                    }
                    // if there's two ending parenthesis, remove one
                    attributeType = attributeType.replaceAll("\\)\\)*", ")");

                    // create attribute and add to list
                    Attribute at = createAttribute(splitAtts[0], attributeType, currentIsPrimary, notnull, unique);
                    if(at == null){
                        this.success = false;
                        break; 
                    }
                    ats.add(at);
                }
                // if there's no primary key after going through all attributes, error
                if(!hasPrimary){
                    System.out.println(input + " does not have a primary key. Cannot create table.");
                    this.success = false;
                }
                else{
                    this.success = true;
                }
            }
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    /**
     * creates an attribute based on the type
     * @param a attribute as a string
     * @param type attribute type as a string
     * @param isPrimary whether tis attribute is the primary
     * @return the newly created attribute or null if attibute type is incorrect
     */
    private Attribute createAttribute(String a, String type, boolean isPrimary, boolean notnull, boolean unique){
        ArrayList<String> descriptors = new ArrayList<>();
        type = type.toLowerCase();
        if (isPrimary){
            descriptors.add("primarykey");
        }
        if (notnull){
            descriptors.add("notnull");
        }
        if (unique){
            descriptors.add("unique");
        }
        Attribute att;
        if (type.equals("integer")) {
            att = new Attribute(type, Integer.SIZE, a, descriptors);
        }
        else if (type.equals("double")) {
            att = new Attribute(type, Double.SIZE, a, descriptors);

        }
        else if (type.equals("boolean")) {
            att = new Attribute(type, 1, a, descriptors);
        }
        else if (type.startsWith("char")) {
            int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
            att = new Attribute(type, (amount * Character.SIZE + 1), a, descriptors);
        }
        else if (type.startsWith("varchar")) {
            int amount = Integer.parseInt(type.substring(type.indexOf("(")+1, type.indexOf(")")).strip());
            att = new Attribute(type, (amount * Character.SIZE + 1), a, descriptors);
        }
        else {
            System.out.println("invalid data type \"" + type + "\"\n");
            return null;
        }
        return att;
    }

    @Override
    public String execute() {
        return sm.createTable(this.name, this.ats);
    }
}
