package src;

import java.io.File;

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

}
