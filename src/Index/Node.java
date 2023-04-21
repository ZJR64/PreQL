package src.Index;

import java.util.HashMap;
import java.util.Map;

public class Node {

    boolean internal;
    Map<Object, Integer> values;

    public Node(boolean isInternal) {
        //TODO make a new node
        values = new HashMap<Object, Integer>();
        this.internal = isInternal;
    }

    public Node(byte[] bytes) {
        //TODO make a node off of bytes
    }

    public Map<Object, Integer> getValues() {
        return values;
    }

    public void setValues(Map<Object, Integer> values) {
        this.values = values;
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
