package com.s2.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO.RowMapper;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO.StatementPreparator;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class S2ReadTable {
    public static void main(String[] args) {

        String s2_host = System.getenv("S2_HOST");
        String s2_password = System.getenv("S2_PASSWORD");

        Pipeline pipeline = Pipeline.create();

        PCollection<KV<Integer, String>> data = pipeline.apply(SingleStoreIO.<KV<Integer, String>>read()
            .withDataSourceConfiguration(DataSourceConfiguration
                .create(s2_host)
                .withUsername("admin")
                .withPassword(s2_password)
                .withDatabase("adtech"))
            .withQuery("SELECT * FROM campaigns_read WHERE campaign_id > ?")
            .withStatementPreparator(new StatementPreparator() {
                public void setParameters(PreparedStatement preparedStatement) throws Exception {
                    preparedStatement.setInt(1, 7);
                }
            })
            .withRowMapper(new RowMapper<KV<Integer, String>>() {
                public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                    return KV.of(resultSet.getInt(1), resultSet.getString(2));
                }
            })
        );

        data
            .apply(MapElements
                .into(TypeDescriptors.strings())
                .via((KV<Integer, String> kv) -> kv.getKey() + "," + kv.getValue()))
            .apply(TextIO
                .write().to("/path/to/s2").withNumShards(1).withSuffix(".csv")
            );

        pipeline.run().waitUntilFinish();

    }
}