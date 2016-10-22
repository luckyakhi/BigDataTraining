package com.veera.MapSideJoins2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {
    private HashMap<Integer, String> productSubCategories = new HashMap<Integer, String>();

    private void readProductSubcategoriesFile(URI uri) throws IOException {
        List<String> lines = FileUtils.readLines(new File(uri));
        for (String line : lines) {
            String[] recordFields = line.split("\\t");
            int key = Integer.parseInt(recordFields[0]);
            String productSubcategoryName = recordFields[2];
            productSubCategories.put(key, productSubcategoryName);
        }
    }

    public void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        readProductSubcategoriesFile(uris[0]);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordFields = value.toString().split("\\t");
        int productId = Integer.parseInt(recordFields[0]);
        int productSubcategoryId = recordFields[18].length() > 0 ? Integer.parseInt(recordFields[18]) : 0;

        String productName = recordFields[1];
        String productNumber = recordFields[2];

        String productSubcategoryName = productSubcategoryId > 0 ? productSubCategories.get(productSubcategoryId) : "";

        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
        ProductRecord record = new ProductRecord(productName, productNumber, productSubcategoryName);
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
    }
}