package src.Index;

public class Bucket {

    private Object primaryKeyValue;
    private int pageNumber;
    private int pageIndex;

    public Bucket(Object primaryKeyValue, int pageNumber, int pageIndex) {
        //TODO make new bucket
        this.primaryKeyValue = primaryKeyValue;
        this.pageNumber = pageNumber;
        this.pageIndex = pageIndex;
    }

    public Bucket(byte[] bytes) {
        //TODO make bucket from bytes
    }
}
