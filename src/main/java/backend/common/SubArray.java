package backend.common;

public class SubArray {
    private byte[] array;
    private int start;
    private int end;

    public SubArray(byte[] array, int start, int end) {
        this.array = array;
        this.start = start;
        this.end = end;
    }

    public byte[] getArray() {
        return array;
    }

    public void setArray(byte[] array) {
        this.array = array;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
