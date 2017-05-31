package edu.uci.ics.textdb.api.Interval;

/**
 * Created by Chang on 5/30/17.
 */
public class Interval {
    private int start;
    private int end;
    private String attributeName;
    public Interval(int start, int end, String attributeName){
        this.start = start;
        this.end = end;
        this.attributeName = attributeName;
    }

    public int getEnd() {
        return end;
    }

    public int getStart() {
        return start;
    }

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public void setStart(int start) {
        this.start = start;
    }
}
