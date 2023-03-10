package src.Commands;

import src.StorageManager.StorageManager;

/**
 * The class for the AlterTable Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class AlterTable extends Command{

    // name of the table to alter
    private String name;
    // type of alter statement
    // will either be "drop" or "add"
    private String alterType;
    // name of the attribute
    private String attributeName;
    // type of attribute
    // will be numm if alterType is drop
    private String attributeType;
    // default value, if present
    // will be null if there's no default or if alterType is drop
    private String defaultValue;

    private StorageManager sm;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public AlterTable(String input, StorageManager sm) {
        super(input);
        this.sm = sm;
    }

    @Override
    public void parse() {
        try {
            String[] splitInput = input.split("(?i)table");
            String command = splitInput[1].strip().replace(";", "");
            String[] splitCommand = command.split(" ");
            this.name = splitCommand[0];
            this.alterType = splitCommand[1].toLowerCase();
            if (!alterType.equals("add") && !alterType.equals("drop")){
                System.out.println(input + " could not be parsed");
                this.success = false;
                return;
            }
            this.attributeName = splitCommand[2];
            // if alterType is add, there will be attributeType and possibly default value
            if(alterType.equals("add")){
                this.attributeType = splitCommand[3];
                if(splitCommand.length >= 6){
                    String value1 = splitCommand[5];
                    int i = 5;
                    // match quotation marks if there's multiple words in string
                    if (value1.startsWith("\"") && !value1.endsWith("\"")) {
                        i++;
                        String next = splitCommand[i];
                        String wholeString = value1.concat(" " + next);
                        while (!next.endsWith("\"")) {
                            i++;
                            next = splitCommand[i];
                            wholeString = wholeString.concat(" " + next);
                        }
                        this.defaultValue = wholeString;
                    }
                    else{
                        this.defaultValue = value1;
                    }
                }
                else{
                    this.defaultValue = null;
                }
            }
            // if drop, altertype and default value are null
            else{
                this.attributeType = null;
                this.defaultValue = null;
            }
            this.success = true;
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        return sm.alterTable(name, alterType, attributeName, attributeType, defaultValue);
    }
}
