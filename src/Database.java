package src;

import java.io.File;
import java.util.Scanner;

public class Database {

    private String db_loc;
    private int buffer_size;
    private Catalog catalog;

    /**
     * Constructor for the database object
     *
     * @param db_loc the location of the database.
     * @param page_size the size of each page of the database.
     * @param buffer_size the size of the buffer.
     */
    public Database(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.buffer_size = buffer_size;
        //create catalog if it does not exist
        String catPath = db_loc + "\\catalog.txt";
        this.catalog = new Catalog(catPath, page_size);
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
    private static void checkCommand(String command, String input) {

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
                System.out.println("The hells a schema?!");
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
    private static void shutDown() {
        //TODO
    }

}
