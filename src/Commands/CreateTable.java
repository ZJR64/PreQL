package src.Commands;

import java.util.HashMap;
import java.util.Map;

public class CreateTable extends Command{

    private Map<String, String> attributesNameToType;
    private String primarykeyName;
    private String name;

    public CreateTable(String input){
        super(input);
    }


    @Override
    public void parse() {
        this.attributesNameToType = new HashMap<>();
        // split on first occurrence of (
        //["create table <name>", attributes,...]
        String[] tokens = input.split("\\(", 2);
        // get the name
        this.name = tokens[0].split("table")[1].strip();

        // parse attributes
        String[] attributes = tokens[1].strip().split(",");
        for (String a : attributes){
            a = a.replace(";", "").strip();
            String[] splitAtts = a.split(" ");
            // if primary key, store that it's the primary key
            if (splitAtts.length > 2 && splitAtts[2].equals("primarykey")){
                this.primarykeyName = splitAtts[0];
            }
            // put each attribute pair in map
            attributesNameToType.put(splitAtts[0], splitAtts[1]);
        }
    }

    @Override
    public String execute() {
        return "ERROR";
    }
}
