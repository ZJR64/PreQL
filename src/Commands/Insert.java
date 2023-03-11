package src.Commands;

import src.StorageManager.StorageManager;
import java.util.ArrayList;


/**
 * The class for the Insert Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class Insert extends Command{

    // table name
    private String name;
    // arraylist of tuples
    // contains array lists of the tuple string values
    private ArrayList<ArrayList<String>> tuples;
    private StorageManager sm;

    // Needs storage manager for execute()
    public Insert(String input, StorageManager sm){
        super(input);
        this.sm = sm;
    }

    /**
     * parses the command and stores the table name, list of tuples,
     * and if the parse was successful
     */
    @Override
    public void parse() {
        try {
            // split on keyword values
            // should get ["insert into <name>", "<tuples>"]
            String[] splitInput = input.split("(?i)values");

            //split first half of input on keyword into and get the name
            this.name = splitInput[0].split("(?i)into")[1].strip();

            // get tuples and remove semicolon
            String tuples = splitInput[1].replace(";", "");
            // split tuples on comma
            String[] splitTuples = tuples.split(",");
            this.tuples = new ArrayList<ArrayList<String>>();

            // iterate though tuples
            for (String tuple : splitTuples) {
                ArrayList<String> currentTuple = new ArrayList<>();
                // remove parenthesis
                tuple = tuple.replace("(", "").replace(")", "").strip();
                // split tuple on space
                String[] splitTuple = tuple.split(" ");
                // iterate through each value, checking for strings with multiple words
                int i = 0;
                while (i < splitTuple.length) {
                    String s = splitTuple[i];
                    // if word starts with quotation but does not end with one,
                    // iterate until you find end of string
                    if (s.startsWith("\"") && !s.endsWith("\"")) {
                        i++;
                        String next = splitTuple[i];
                        String wholeString = s.concat(" " + next);
                        while (!next.endsWith("\"")) {
                            i++;
                            next = splitTuple[i];
                            wholeString = wholeString.concat(" " + next);
                        }
                        currentTuple.add(wholeString);
                    }
                    else if(s.startsWith("\"") && s.endsWith("\"")){
                        s = s.replaceAll("\"", "");
                        currentTuple.add(s);
                    }
                    else {
                        currentTuple.add(s);
                    }
                    i++;
                }
                this.tuples.add(currentTuple);
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
        return sm.insert(name, tuples);
    }
//test
}
