package src;

import java.io.File;

public class Catalog {

    private String catPath;
    private int pageSize;

    /**
     * Constructor for the Catalog object
     *
     * @param catPath the file path to the catalog.
     * @param pageSize the size of each page of the database.
     */
    public Catalog(String catPath, int pageSize) {
        this.catPath = catPath;
        if (Helper.checkFile(catPath)) {
            System.out.println("catalog found.");
            //TODO make the catalog read from file
            this.pageSize = pageSize;
        } else {
            try {
                File newFile = new File(catPath);
                newFile.createNewFile();
                System.out.println("catalog file created.");
            } catch (Exception e) {
                System.err.println("Error: catalog file could not be created");
                return;
            }
            //assign values
            this.pageSize = pageSize;
        }
    }

    /**
     * Getter for page
     */
    public int getPageSize() {
        return pageSize;
    }

    public void shutDown(){
        //TODO write to file
    }
}
