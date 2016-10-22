package com.veera.ReduceSideJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinSortingComparator extends WritableComparator {
    public JoinSortingComparator()
    {
        super(ProductIdKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ProductIdKey first = (ProductIdKey) a;
        ProductIdKey second = (ProductIdKey) b;

        return first.compareTo(second);
    }
}