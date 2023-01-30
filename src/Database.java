package src;

import java.io.File;
import java.util.Scanner;

public class Database {

    private String db_loc;
    private int page_size;
    private int buffer_size;

    /**
     * Creates a new Database object.
     *
     * @param db_loc the location of the database.
     * @param page_size the size of each page of the database.
     * @param buffer_size the size of the buffer.
     */
    public Database(String db_loc, int page_size, int buffer_size) {
        this.db_loc = db_loc;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
    }

    public void run(){
        //check for file and create if not present
        //PLACEHOLDER
        if (Helper.checkFile(db_loc + "\\database.db")) {
            System.out.println("database file found.");
        }
        else {
            try {
                File newFile = new File(db_loc + "\\database.db");
                newFile.createNewFile();
                System.out.println("database file created.");
            } catch (Exception e) {
                System.err.println("Error: database file could not be created");
                return;
            }
        }

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
                    System.out.println("loser");
                    return;
                }
            }

            //split string
            input = input.substring(0,  input.length() - 1);
            String[] line = input.split(",");
            String[] firstLine = input.split(" ");
            String command = firstLine[0];
            if (firstLine[0].equalsIgnoreCase("create") || firstLine[0].equalsIgnoreCase("display")) {
                command += " " + firstLine[1];
            }

            //check for ; at end of command
            if (input.substring(input.length() - 1).equals(";")) {

            }

            //check command
            checkCommand(command, input);
        }
    }

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

}
