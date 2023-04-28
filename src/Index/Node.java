package src.Index;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Node {

    boolean internal;
    Integer parent;
    Map<Object, Integer> pageNums;
    Map<Object, Integer> indexes;
    Integer finalValue;

    public Node(boolean isInternal, Integer parent) {
        //TODO make a new node
        pageNums = new HashMap<Object, Integer>();
        indexes = new HashMap<Object, Integer>();
        finalValue = null;
        this.internal = isInternal;
        this.parent = parent;
    }

    public Node(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        //get isInternal
        int internalNum = buffer.getInt();
        if (internalNum == 0) {
            this.internal = false;
        }
        else {
            this.internal = true;
        }

        //get parent
        this.parent = buffer.getInt();

        //get number of pageNums
        int numValues = buffer.getInt();

        //get pageNums
        for (int i = 0; i < numValues; i++) {
            //TODO
            //convert bytes to object
            //store in map
        }

        //get number of indexes
        numValues = buffer.getInt();

        //get indexes
        for (int i = 0; i < numValues; i++) {
            //TODO
            //convert bytes to object
            //store in map
        }

        //get final value
        this.finalValue = buffer.getInt();
    }

    public Map<Object, Integer> getPageNums() {
        return pageNums;
    }

    public void setPageNums(Map<Object, Integer> pageNums) {
        this.pageNums = pageNums;
    }

    public Map<Object, Integer> getIndexes() {
        return pageNums;
    }

    public void setIndexes(Map<Object, Integer> indexes) {
        this.indexes = indexes;
    }

    public Integer getFinalValue() {
        return finalValue;
    }

    public void setFinalValue(Integer finalValue) {
        this.finalValue = finalValue;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean isInternal) {
        this.internal = isInternal;
    }

    public Integer getParent() {
        return parent;
    }

    public void setParent(Integer parent) {
        this.parent = parent;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[getNodeByteSize()]);

        //set isInternal
        if (this.internal) {
            buffer.putInt(1);
        }
        else {
            buffer.putInt(0);
        }

        //set parent
        buffer.putInt(this.parent);

        //set number of pageNums
        buffer.putInt(pageNums.size());

        //set pageNums
        for (Map.Entry<Object, Integer> entry : pageNums.entrySet()) {
            putObjectBytes(buffer, entry.getKey());
            //set page num
            buffer.putInt(entry.getValue());
        }

        //set number of pageNums
        buffer.putInt(indexes.size());

        //set indexes
        for (int i = 0; i < indexes.size(); i++) {
            //TODO
            //convert bytes to object
            //store in database
        }

        //set final value
        buffer.putInt(finalValue);

        return buffer.array();
    }

    public void putObjectBytes(ByteBuffer buffer, Object keyValue) {
        //TODO
    }

    public int getNodeByteSize() {
        int size = 0;

        //go through entire file
        for (Map.Entry<Object, Integer> entry : this.getPageNums().entrySet()) {
            //TODO store primary object
            size += Integer.BYTES;
        }

        //add final value
        if (finalValue != null) {
            size += Integer.BYTES;
        }

        //return size
        return size;
    }
}
