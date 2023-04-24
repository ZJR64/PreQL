package src.Index;

import java.util.HashMap;
import java.util.Map;

public class Node {

    boolean internal;
    Map<Object, Integer> pageNums;
    Map<Object, Integer> indexes;
    Integer FinalValue;

    public Node(boolean isInternal) {
        //TODO make a new node
        pageNums = new HashMap<Object, Integer>();
        indexes = new HashMap<Object, Integer>();
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
}
