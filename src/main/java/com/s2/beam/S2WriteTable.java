package com.s2.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO.UserDataMapper;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class S2WriteTable {
    public static void main(String[] args) {

        String s2_host = System.getenv("S2_HOST");
        String s2_password = System.getenv("S2_PASSWORD");

        Pipeline pipeline = Pipeline.create();

        PCollection<String> lines = pipeline.apply(
            TextIO.read().from("/path/to/s2-00000-of-00001.csv"));

        PCollection<KV<Integer, String>> keyValues = lines.apply(
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                .via((String line) -> {
                    String[] fields = line.split(",");
                    return KV.of(Integer.parseInt(fields[0]), fields[1]);
                })
        );

        keyValues.apply(SingleStoreIO.<KV<Integer, String>>write()
            .withDataSourceConfiguration(DataSourceConfiguration
                .create(s2_host)
                .withUsername("admin")
                .withPassword(s2_password)
                .withDatabase("adtech"))
            .withTable("campaigns_write")
            .withUserDataMapper(new UserDataMapper<KV<Integer, String>>() {
                public List<String> mapRow(KV<Integer, String> element) {
                    List<String> result = new ArrayList<>();
                    result.add(element.getKey().toString());
                    result.add(element.getValue());
                    return result;
                }
            })
        );

        pipeline.run().waitUntilFinish();

    }
}