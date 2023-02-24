package src;

/**
 * The class for the page buffer. Uses a LRU (Least Recently Used) method for
 * determining the page to write to hardware first.
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class PageBuffer {

    private final int bufferSize;

    /**
     * Constructor for the page buffer object.
     *
     * @param bufferSize the fixed size of the buffer.
     */
    public PageBuffer(int bufferSize){
        this.bufferSize = bufferSize;

    }

    /**
     * Fetches a page from hardware and writes it to the buffer.
     */
    public void writeToBuffer(){

    }


    /**
     * Employs LRU to determine the page to write back to hardware when
     * more buffer space is needed.
     */
    public void writeToHardware(){

    }

    /**
     * If/when the database is shut down, writeBackAll writes all the pages
     * back to hardware.
     */
    public void writeBackAll(){

    }

}
