package src.StorageManager;

/**
 * Defines the page object.
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class Page {

    private final int id;
    private final int size;

    /**
     *
     * @param id the unique identifier of the page.
     * @param size the fixed size of the page.
     */
    public Page(int id, int size){
        this.id = id;
        this.size = size;
    }

    /**
     * Gets the id of the page.
     * @return the id of the page.
     */
    public int getId(){
        return id;
    }

    /**
     * Gets the size of the page.
     * @return the size of the page.
     */
    public int getSize() {
        return size;
    }
}