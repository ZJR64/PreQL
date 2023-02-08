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

    public Command(String input) {
        this.input = input;
        breakUp();
    }

    private static void breakUp() {
        //TODO
    }
    public String execute() {
        return "FOOL";
    }
}
