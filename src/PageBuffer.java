package src;

import java.util.ArrayList;

/**
 * The class for the page buffer. Uses a LRU (Least Recently Used) method for
 * determining the page to write to hardware first. The buffer's array is
 * ordered in terms of last access time, with the most recently accessed
 * element being first, and the least recently accessed element being last.
 *
 * @author Jackson O'Connor jdo7339@rit.edu
 */
public class PageBuffer {

    private final int bufferSize;
    private int[] buffer;

    /**
     * Constructor for the page buffer object.
     *
     * @param bufferSize the fixed size of the buffer.
     */
    public PageBuffer(int bufferSize){
        this.bufferSize = bufferSize;
        buffer = new int[bufferSize];


    }

    /**
     * Checks to see if the buffer is full. If the buffer is full, space is
     * cleared and the page is written to the buffer. If not, simply writes
     * to the buffer.
     */
    public void writeToBuffer(int pageId){
        for(int i = 0; i < bufferSize; i++){
            if(buffer[i] == 0) {
                if(i == 0){
                    buffer[i] = pageId;
                    return;
                }
                System.arraycopy(buffer, 0, buffer, 1, i );
                buffer[0] = pageId;
                return;
            }
        }
        writeToHardware();
        buffer[0] = pageId;

    }


    /**
     * Employs LRU to determine the page to write back to hardware when
     * more buffer space is needed.
     */
    public void writeToHardware(){

    }

    /**
     * If/when the database is shut down, bufferPurge writes all the pages
     * back to hardware.
     */
    public void bufferPurge(){

    }


    public int getPage(int pageId){

        return 0;
    }

}
