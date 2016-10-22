package com.veera.ReduceSideJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProductIdKey implements WritableComparable<ProductIdKey> {
    public static final IntWritable PRODUCT_RECORD = new IntWritable(0);
    public static final IntWritable DATA_RECORD = new IntWritable(1);
    public IntWritable productId = new IntWritable();
    public IntWritable recordType = new IntWritable();

    public ProductIdKey() {
    }

    public ProductIdKey(int productId, IntWritable recordType) {
        this.productId.set(productId);
        this.recordType = recordType;
    }

    public void write(DataOutput out) throws IOException {
        this.productId.write(out);
        this.recordType.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.productId.readFields(in);
        this.recordType.readFields(in);
    }

    public int compareTo(ProductIdKey other) {
        if (this.productId.equals(other.productId)) {
            return this.recordType.compareTo(other.recordType);
        }
        else {
            return this.productId.compareTo(other.productId);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((productId == null) ? 0 : productId.hashCode());
        result = prime * result + ((recordType == null) ? 0 : recordType.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ProductIdKey other = (ProductIdKey) obj;
        if (productId == null) {
            if (other.productId != null) return false;
        }
        else if (!productId.equals(other.productId)) return false;
        if (recordType == null) {
            if (other.recordType != null) return false;
        }
        else if (!recordType.equals(other.recordType)) return false;
        return true;
    }

}
