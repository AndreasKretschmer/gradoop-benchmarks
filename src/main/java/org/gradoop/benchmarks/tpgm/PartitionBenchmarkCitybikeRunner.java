package org.gradoop.benchmarks.tpgm;

import org.apache.xerces.xs.StringList;

import java.util.ArrayList;
import java.util.Arrays;

public class PartitionBenchmarkCitybikeRunner {
    private static final String[] vertex_partition_starts = new String [] {"hash", "range", "default"};
    private static final String[] edge_partition_starts = new String [] {"edgeHash", "edgeRange", "DBH"};

    private static final String[] vertex_partition_fields = new String[] {"id", "name", "regionId"};
    private static final String[] edge_partition_fields = new String[] {"id", "gender", "bike_id"};
    private static final String[] queries_pattern_matching_citybike = new String[] {
            "MATCH (s1:station)-[t:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE t.bike_id = t2.bike_id",
            "MATCH (s1:station)-[t1:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE s1.id = s2.id",
            "MATCH (v1:Station {cellId: 2883})-[t1:Trip]->(v2:Station)-[t2:Trip]->(v3:Station) " +
            "WHERE v2.id != v1.id " +
            "AND v2.id != v3.id " +
            "AND v3.id != v1.id " +
            "AND t1.val.precedes(t2.val) " +
            "AND t1.val.lengthAtLeast(Minutes(30)) " +
            "AND t2.val.lengthAtLeast(Minutes(30))"};

    public static void main(String[] args) throws Exception {
        runSnapshotBenchmark(args);
        runPatternMatchingBenchmark(args);
    }

    private static void runPatternMatchingBenchmark(String[] args) throws Exception{
        ArrayList<String> stringList = new ArrayList<>(Arrays.asList(args));
        stringList.add("-f");
        stringList.add("csv");
        stringList.add("-y");
        stringList.add("all");

        for (String strat : vertex_partition_starts) {
            for (String partition_field : vertex_partition_fields) {
                for (String query : queries_pattern_matching_citybike) {
                    PatternMatchingBenchmark.SetQueryString(query);
                    stringList.add("-ps");
                    stringList.add(strat);
                    stringList.add("-pf");
                    stringList.add(partition_field);
                    args = new String [stringList.size()];
                    args = stringList.toArray(args);
                    PatternMatchingBenchmark.main(args);
                }
            }
        }
        for (String strat : edge_partition_starts) {
            for (String partition_field : edge_partition_fields) {
                for (String query : queries_pattern_matching_citybike) {
                    PatternMatchingBenchmark.SetQueryString(query);
                    stringList.add("-ps");
                    stringList.add(strat);
                    stringList.add("-pf");
                    stringList.add(partition_field);
                    args = new String [stringList.size()];
                    args = stringList.toArray(args);
                    PatternMatchingBenchmark.main(args);
                }
            }
        }
    }

    private static void runSnapshotBenchmark(String[] args) throws Exception{
        ArrayList<String> stringList = new ArrayList<>(Arrays.asList(args));
        stringList.add("-y");
        stringList.add("all");

        for (String strat : vertex_partition_starts) {
            for (String partition_field : vertex_partition_fields) {
                stringList.add("-ps");
                stringList.add(strat);
                stringList.add("-pf");
                stringList.add(partition_field);
                args = new String [stringList.size()];
                args = stringList.toArray(args);
                SnapshotBenchmark.main(args);
            }
        }

        for (String strat : edge_partition_starts) {
            for (String partition_field : edge_partition_fields) {
                stringList.add("-ps");
                stringList.add(strat);
                stringList.add("-pf");
                stringList.add(partition_field);
                args = new String [stringList.size()];
                args = stringList.toArray(args);
                SnapshotBenchmark.main(args);
            }
        }
    }
}
