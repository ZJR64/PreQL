package src.Commands;

import java.util.ArrayList;

public class Insert extends Command{

    private String name;
    private ArrayList tuples;

    public Insert(String input){
        super(input);
    }

    @Override
    public void parse() {
        // split on keyword values
        // should get ["insert into <name>", "<tuples>"]
        String[] splitInput = input.split("(?i)values");

        //split first half of input on keyword into and get the name
        this.name = splitInput[0].split("(?i)into")[1];

        // get tuples and remove semicolon
        String tuples = splitInput[1].replace(";", "");
        // split tuples on comma
        String[] splitTuples = tuples.split(",");
        this.tuples = new ArrayList();

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
                } else {
                    currentTuple.add(s);
                }
                i++;
            }
            this.tuples.add(currentTuple);
        }
    }

    @Override
    public String execute() {
        return "ERROR";
    }

}
