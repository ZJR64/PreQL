package src;

import src.Catalog.Catalog;
import src.Catalog.Schema;
import src.Commands.DisplaySchema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
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

    private final String databaseName = "PrQL";
    private String location;
    private String dbFile;
    private int bufferSize;
    private int pageSize;
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
        this.dbFile = location + "\\db";

        System.out.println("Welcome to " + databaseName);

        //check for existing database
        System.out.println("Looking at " + location + " for existing db....");

        if (Helper.checkFile(dbFile)) {
            System.out.println("Database found...");
            System.out.println("Restarting the database...");
            System.out.println("\tIgnoring provided pages size, using stored page size");

            //get bytes for page size
            byte[] byteArray;
            try {
                FileInputStream inputStream = new FileInputStream(dbFile);;
                byteArray = inputStream.readAllBytes();
                inputStream.close();
            } catch (Exception e) {
                System.err.println("Error: db file could not be read");
                return;
            }
            //extract info
            byte[] pageSizeArray = {byteArray[0], byteArray[1], byteArray[2], byteArray[3]};
            ByteBuffer byteBuffer = ByteBuffer.wrap(pageSizeArray);
            this.pageSize = byteBuffer.getInt();
            byteBuffer.clear();
        }
        else {
            System.out.println("No existing db found");
            System.out.println("Creating new db at " + location);

            //create file
            try {
                byte[] pageBytes= new byte[4];
                FileOutputStream outputStream = new FileOutputStream(dbFile);
                //store name size
                for (int i = 0; i < 4; i++) {
                    pageBytes[i] = (byte) (pageSize >>> (24 - (8 * i)));
                }
                outputStream.write(pageBytes);
                outputStream.flush();
                outputStream.close();
            } catch (Exception e) {
                System.err.println("Error: db file could not be created");
                System.err.println(e);
            }

            this.pageSize = pageSize;
            System.out.println("New db created successfully");
        }

        System.out.println("Page Size: " + pageSize);
        System.out.println("Buffer Size: " + bufferSize);


        //create catalog
        String catPath = location + "\\catalog";
        this.catalog = new Catalog(catPath, pageSize);
    }

    /**
     * Main method for the database. Takes and processes input given by the user.
     * runs until the user types 'quit'.
     */
    public void run(){
        //create new scanner
        Scanner reader = new Scanner(System.in);

        System.out.println("\nPlease enter commands, enter <quit> to shutdown the db");

        //wait for user input
        while(true) {
            System.out.print("\n" + databaseName + ">");
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
                DisplaySchema action = new DisplaySchema(input, location, pageSize, bufferSize, this.catalog);
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
        //record catalog
        this.catalog.shutDown();
    }

}
