package src.Index;

import java.util.HashMap;
import java.util.Map;

public class Node {

    boolean internal;
    Map<Object, Integer> pageNums;
    Map<Object, Integer> indexes;
    Integer finalValue;

    public Node(boolean isInternal) {
        //TODO make a new node
        pageNums = new HashMap<Object, Integer>();
        indexes = new HashMap<Object, Integer>();
        finalValue = null;
        this.internal = isInternal;
    }

    public Node(byte[] bytes) {
        //TODO make a node off of bytes
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

    public byte[] toBytes() {
        //TODO write node to bytes
        return null;
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
