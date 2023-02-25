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
    private Map<String, String> attributesNameToType;
    // name of the primary key
    private String primarykeyName;
    // name of the table to create
    private String name;

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
        try{
            this.attributesNameToType = new HashMap<>();
            // split on first occurrence of (
            //["create table <name>", attributes,...]
            String[] tokens = input.split("\\(", 2);
            // get the name
            this.name = tokens[0].split("(?i)table")[1].strip();

            // parse attributes
            String[] attributes = tokens[1].strip().split(",");
            for (String a : attributes) {
                a = a.replace(";", "").strip();
                a = a.replace(")", "").strip();

                String[] splitAtts = a.split(" ");
                // if primary key, store that it's the primary key
                if (splitAtts.length > 2 && splitAtts[2].equalsIgnoreCase("primarykey")) {
                    this.primarykeyName = splitAtts[0];
                }
                // put each attribute pair in map
                attributesNameToType.put(splitAtts[0], splitAtts[1]);
                this.success = true;
            }
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        ArrayList<Attribute> ats = new ArrayList<>();
        boolean primenums = false;
        for (String s : this.attributesNameToType.keySet()) {
            String type = this.attributesNameToType.get(s);
            ArrayList<String> descriptors = new ArrayList<>();
            int datatype = 0;
            if (s.equals(this.primarykeyName)){
                if(primenums == true){
                    return "ERROR";
                }
                descriptors.add("primarykey");
                primenums = true;

            }
            if(type.equals( "integer")){
                ats.add(new Attribute(type, Integer.SIZE, s, descriptors));
                datatype = 1;
            }
            if(type.equals( "double")){
                ats.add(new Attribute(type, Double.SIZE, s, descriptors));
                datatype = 1;

            }
            if(type.equals( "boolean")){
                ats.add(new Attribute(type, 1, s, descriptors));
                datatype = 1;

            }
            if(type.startsWith("char")){
                int amount = Integer.parseInt(String.valueOf(type.charAt(4)));
                ats.add(new Attribute(type, (amount * Character.SIZE + 1), s, descriptors));
                datatype = 1;

            }
            if(type.startsWith("varchar")){
                int amount = Integer.parseInt(String.valueOf(type.charAt(8)));
                ats.add(new Attribute(type, (amount * Character.SIZE + 1), s, descriptors));
                datatype = 1;
            }
            if (datatype == 0){
                return "invalid data type \"" + type + "\"\n ERROR";
            }

        }
        if(primenums == false){
            return "ERROR";
        }
        return sm.createTable(this.name, ats);
    }

}
