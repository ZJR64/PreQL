package src.Commands;

/**
 * The class for all commands. This class should be used to help keep
 * commands as uniform as possible and help with implementation elsewhere.
 * Any command class created should extend this class so it can use it's features
 *
 * @author Zak Rutherford zjr6302@rit.edu
 * @author Kaitlyn DeCola kmd8594@rit.edu
 */
public abstract class Command {
    String input;

    boolean success;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public Command(String input) {
        this.input = input;
        parse();
    }

    /**
     * A method that will break up and the user input.
     */
    public abstract void parse();

    /**
     * A placeholder method so classes that extend this one can be run by the
     * Database class. Returns "ERROR" when incorrect input is given by the user.
     *
     * @return error because this is only executed with bad input
     */
    public abstract String execute();

    /**
     * returns the success variable that is set when class is parsed
     * @return boolean whether the parse was successful
     */
    public boolean isSuccess() {
        return success;
    }
}
