package resources;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Rankings implements Serializable {

    private static final long serialVersionUID = -1549827195410578903L;
    private static final int DEFAULT_COUNT = 10;

    private final int maxSize;
    private final List<Rankable> rankedItems = Collections.synchronizedList(new ArrayList<Rankable>());

    public Rankings() {
        this(DEFAULT_COUNT);
    }

    public Rankings(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1");
        }
        maxSize = topN;
    }

    /**
     * @return the maximum possible number (size) of ranked objects this instance can hold
     */
    public int maxSize() {
        return maxSize;
    }

    /**
     * @return the number (size) of ranked objects this instance is currently holding
     */
    public int size() {
        return rankedItems.size();
    }

    public List<Rankable> getRankings() {
        return defensiveCopyOf(rankedItems);
    }

    private List<Rankable> defensiveCopyOf(List<Rankable> list) {
        return Lists.newArrayList(rankedItems);
    }

    public void updateWith(Rankings other) {
        for (Rankable r : other.getRankings()) {
            updateWith(r);
        }
    }

    public void updateWith(Rankable r) {
        synchronized (rankedItems) {
            addOrReplace(r);
            rerank();
            shrinkRankingsIfNeeded();
        }
    }

    private void addOrReplace(Rankable r) {
        Integer rank = findRankOf(r);
        if (rank != null) {
            rankedItems.set(rank, r);
        } else {
            rankedItems.add(r);
        }
    }

    private Integer findRankOf(Rankable r) {
        Object tag = r.getObject();
        for (int rank = 0; rank < rankedItems.size(); rank++) {
            Object cur = rankedItems.get(rank).getObject();
            if (cur.equals(tag)) {
                return rank;
            }
        }
        return null;
    }

    private void rerank() {
        Collections.sort(rankedItems);
        Collections.reverse(rankedItems);
    }

    private void shrinkRankingsIfNeeded() {
        if (rankedItems.size() > maxSize) {
            rankedItems.remove(maxSize);
        }
    }

    /**
     * Removes ranking entries that have a count of zero.
     */
    public void pruneZeroCounts() {
        int i = 0;
        while (i < rankedItems.size()) {
            if (rankedItems.get(i).getCount() == 0) {
                rankedItems.remove(i);
            } else {
                i++;
            }
        }
    }

    public String toString() {

        return rankedItems.toString();
    }
}