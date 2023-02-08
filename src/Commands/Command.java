package src.Commands;

/**
 * The class for all commands. This class should be used to help keep
 * commands as uniform as possible and help with implementation elsewhere.
 * Any command class created should extend this class so it can use it's features
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Command {
    String input;
    String[] filtered;

    /**
     * Constructor for the Command object. Used to store  and manipulate the input from
     * the classes that extend this one.
     *
     * @param input the entire input from the user.
     */
    public Command(String input) {
        this.input = input;
        breakUp();
    }

    /**
     * A method that will break up and the user input. May be to hyper specific
     * for more complicated classes down the line.
     */
    private static void breakUp() {
        //TODO
    }

    /**
     * A placeholder method so classes that extend this one can be run by the
     * Database class. Returns "ERROR" when incorrect input is given by the user.
     *
     * @return error because this is only executed with bad input
     */
    public String execute() {
        return "ERROR";
    }
}