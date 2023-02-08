package src;

import src.Catalog.Catalog;
import src.Commands.DisplaySchema;

import java.util.Scanner;

/**
 * The class for the database. This is the class that does most of the heavy lifting.
 * It creates the catalog object and takes in user input. It also sorts through the
 * input to determine the command that needs to be executed. Finally, when all is done,
 * it safely shuts down and stores the database.
 *
 * @author Zak Rutherford zjr6302@rit.edu
 */
public class Database {

    private String location;
    private int bufferSize;
    private Catalog catalog;

    /**
     * Constructor for the database object
     *
     * @param location the location of the database.
     * @param pageSize the size of each page of the database.
     * @param bufferSize the size of the buffer.
     */
    public Database(String location, int pageSize, int bufferSize) {
        this.location = location;
        this.bufferSize = bufferSize;
        //create catalog if it does not exist
        String catPath = location + "\\catalog";
        this.catalog = new Catalog(catPath, pageSize);
    }

    public void run(){
        //create new scanner
        Scanner reader = new Scanner(System.in);

        //wait for user input
        while(true) {
            String input = "";
            while (input.equals("") || !input.substring(input.length() - 1).equals(";")) {
                String line = reader.nextLine();
                input += line;
                //check for quit
                if (line.equalsIgnoreCase("quit")) {
                    shutDown();
                    return;
                }
            }

            //split string
            input = input.substring(0,  input.length() - 1);
            String[] line = input.split(",");
            String[] firstLine = line[0].split(" ");
            String command = firstLine[0];
            if (firstLine[0].equalsIgnoreCase("create") || firstLine[0].equalsIgnoreCase("display")) {
                command += " " + firstLine[1];
            }

            /*
            //check for ; at end of command
            if (command.substring(input.length() - 1).equals(";")) {
                command = command.substring(0, input.length() - 1);
                System.out.println(command);
            }
             */

            //check command
            checkCommand(command, input);
        }
    }

    /**
     * Checs the start of the commands from the users then sends the input to
     * the right classes.
     *
     * @param command the reformatted first parts of the user input.
     * @param input the entire input from the user.
     */
    private void checkCommand(String command, String input) {

        switch (command.toLowerCase()) {
            case "create table":
                System.out.println("what? does it look like i'm a carpenter?");
                return;

            case "select":
                System.out.println("Select this! *censored action*");
                return;

            case "insert":
                System.out.println("how about you insert this up your... hmm.. I'm blanking on where you should put this.");
                return;

            case "display schema":
                DisplaySchema action = new DisplaySchema(input, location, bufferSize, this.catalog);
                return;

            case "display info":
                System.out.println("Info?! Well did you know the canary islands were named after dogs?... or seals. Latin is confusing.");
                return;

            default:
                System.out.println("*mockingly* " + input);
        }

    }

    /**
     * When the database is being powered down it will store the buffer
     * and ensure the catalog is updated.
     */
    private void shutDown() {
        //TODO
        this.catalog.shutDown();
    }

}
