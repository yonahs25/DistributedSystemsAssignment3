package ass3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A simple class that represents a pair of long values and implements the Hadoop Writable and Comparable interfaces.
 */
public class LongPairWritable implements WritableComparable<LongPairWritable> {

    private long first;
    private long second;

    /**
     * Default constructor.
     */
    public LongPairWritable() {
        first = 0;
        second = 0;
    }

    /**
     * Parameterized constructor.
     * @param first the first long value in the pair.
     * @param second the second long value in the pair.
     */
    public LongPairWritable(long first, long second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Writes the pair to a data output.
     * @param out the data output to which to write.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    /**
     * Reads a pair from a data input.
     * @param in the data input from which to read.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }

    /**
     * Returns the String representation of the pair.
     * @return the String representation of the pair.
     */
    @Override
    public String toString() {
        return first + " " + second;
    }

    /**
     * Returns the first long value in the pair.
     * @return the first long value in the pair.
     */
    public long getFirst() {
        return first;
    }

    /**
     * Sets the first long value in the pair.
     * @param first the first long value to set in the pair.
     */
    public void setFirst(long first) {
        this.first = first;
    }

    /**
     * Returns the second long value in the pair.
     * @return the second long value in the pair.
     */
    public long getSecond() {
        return second;
    }

    /**
     * Sets the second long value in the pair.
     * @param second the second long value to set in the pair.
     */
    public void setSecond(long second) {
        this.second = second;
    }

    /**
     * Compares this pair with another LongPairWritable instance.
     * @param other the other LongPairWritable instance to compare with.
     * @return 0 if the first long values in the pairs are equal;
     *         -1 if this LongPairWritable's first long value is smaller than the other's;
     *         1 otherwise.
     */
    @Override
    public int compareTo(LongPairWritable other) {
        if (first == other.first) {
            return 0;
        } else if (first < other.first) {
            return -1;
        } else {
            return 1;
        }
    }
}
