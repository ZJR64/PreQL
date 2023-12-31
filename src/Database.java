package src;

import src.Catalog.Catalog;
import src.Catalog.Schema;
import src.Commands.*;
import src.StorageManager.*;

import javax.swing.*;
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
    private boolean indexing;
    private int bufferSize;
    private int pageSize;
    private Catalog catalog;
    private StorageManager storageManager;

    /**
     * Constructor for the database object
     *
     * @param location the location of the database.
     * @param pageSize the size of each page of the database.
     * @param bufferSize the size of the buffer.
     * @param indexing whether this database uses indexing or not.
     */
    public Database(String location, int pageSize, int bufferSize, boolean indexing) {
        this.location = location;
        this.bufferSize = bufferSize;
        this.indexing = indexing;
        this.dbFile = location + File.separator + "db";
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
            ByteBuffer buffer = ByteBuffer.wrap(byteArray);
            this.pageSize = buffer.getInt();

            //get indexing
            int indexNum = buffer.getInt();
            if (indexNum == 0) {
                this.indexing = false;
            }
            else {
                this.indexing = true;
            }
        }
        else {
            System.out.println("No existing db found");
            System.out.println("Creating new db at " + location);

            //create file
            try {
                ByteBuffer buffer = ByteBuffer.wrap(new byte[Integer.BYTES * 2]);
                FileOutputStream outputStream = new FileOutputStream(dbFile);
                //store page size
                buffer.putInt(pageSize);
                if (this.indexing) {
                    buffer.putInt(1);
                }
                else {
                    buffer.putInt(0);
                }
                outputStream.write(buffer.array());
                outputStream.flush();
                outputStream.close();
            } catch (Exception e) {
                System.err.println("Error: db file could not be created");
                System.err.println(e);
            }

            this.pageSize = pageSize;
            System.out.println("New db created successfully");
        }

        System.out.println("Page Size: " + this.pageSize);
        System.out.println("Buffer Size: " + this.bufferSize);

        //create catalog
        String catPath = location + "\\catalog";
        this.catalog = new Catalog(catPath, pageSize, indexing, new BufferManager(pageSize, bufferSize, location));
        BufferManager bm = new BufferManager(pageSize, bufferSize, location);
        this.storageManager = new StorageManager(bm, this.catalog, this.indexing);
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
                if (!input.equalsIgnoreCase("")) {
                    input += " ";           //temporary fix to the newline problem
                }
                String line = reader.nextLine();
                input += line;
                //check for quit
                if (line.equalsIgnoreCase("quit")) {
                    shutDown();
                    return;
                }
            }
            Command action = checkCommand(input);
            if(action != null) {
                System.out.println(action.execute());
            }
        }
    }

    /**
     * Checks the start of the commands from the users then sends the input to
     * the right classes.
     *
     * @param input the entire input from the user.
     */
    private Command checkCommand(String input) {
        Command action;
        input = input.strip();

        if(input.toLowerCase().startsWith("create table")){
            action = new CreateTable(input, storageManager);
        }
        else if(input.toLowerCase().startsWith("select")){
            action = new Select(input, storageManager);
        }

        else if(input.toLowerCase().startsWith("insert")){
            action = new Insert(input, storageManager);
        }

        else if(input.toLowerCase().startsWith("display schema")){
            action = new DisplaySchema(input, location, pageSize, bufferSize, this.catalog);
        }

        else if(input.toLowerCase().startsWith("display info")){
            action = new DisplayInfo(input, this.catalog);
        }
        else if(input.toLowerCase().startsWith("drop")){
            action = new DropTable(input, storageManager);
        }
        else if(input.toLowerCase().startsWith("alter table")){
            action = new AlterTable(input, storageManager);
        }
        else if(input.toLowerCase().startsWith("update")){
            action = new Update(input, storageManager);
        }
        else if(input.toLowerCase().startsWith("delete")){
            action = new Delete(input, storageManager);
        }

        else{
            System.out.println(input + " is not a recognised command \nERROR");
            return null;
        }
        // if the input was able to be parsed, return comand
        // otherwise, return null
        if (action.isSuccess()){
            return action;
        }
        else{
            return null;
        }

    }

    /**
     * When the database is being powered down it will store the buffer
     * and ensure the catalog is updated.
     */
    private void shutDown() {
        //record catalog
        System.out.println("Safely shutting down the database...");
        System.out.println("Purging page buffer...");
        this.storageManager.bm.shutDown();
        System.out.println("Saving catalog...");
        this.catalog.shutDown();
        System.out.println("Exiting the database...");
    }

}
