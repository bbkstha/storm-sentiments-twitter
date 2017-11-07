package resources;
public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getCount();

}
