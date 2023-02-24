package src.Commands;

/**
 * The class for the Select Command.
 * It takes the input as the argument and parses it and executes the command.
 *
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public class Select extends Command {
    // table name
    private String name;

    // default constructor
    public Select(String input){
        super(input);
    }

    /**
     * parses the command and stores the table name,
     * and if the parse was successful
     */
    @Override
    public void parse(){
        try {
            // split on the keyword from
            // should return ["select *", <name>]
            String[] split = input.split("(?i)from");
            // remove semicolon and white space
            this.name = split[1].replace(";", "").strip();
            this.success = true;
        }
        catch(Exception e){
            System.out.println(input + " could not be parsed");
            this.success = false;
        }
    }

    @Override
    public String execute() {
        return "ERROR";
    }
}
