package org.gradoop.benchmarks.tpgm;

import java.util.ArrayList;
import java.util.Arrays;

public class PartitionBenchmarkLdbcRunner {
    private static final String[] vertex_partition_starts = new String [] {"hash", "range", "default"};
    private static final String[] edge_partition_starts = new String [] {"edgeHash", "edgeRange", "DBH"};

    private static final String[] vertex_partition_fields = new String[] {"id", "name"};
    private static final String[] edge_partition_fields = new String[] {"id"};
    private static final String[] queries_pattern_matching_ldbc = new String[] {"MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post)",
            "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) WHERE l.val_from.after(Timestamp(2012-06-01)) AND l.val_from.before(Timestamp(2012-06-02))",
            "MATCH (p:person)-[s:studyAt]->(u:university)"};


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
        args = new String [stringList.size()];
        args = stringList.toArray(args);

        for (String strat : vertex_partition_starts) {
            for (String partition_field : vertex_partition_fields) {
                for (String query : queries_pattern_matching_ldbc) {
                    PatternMatchingBenchmark.SetQueryString(query);
                    PatternMatchingBenchmark.SetPartStrat(strat);
                    PatternMatchingBenchmark.SetPartField(partition_field);
                    PatternMatchingBenchmark.main(args);
                }
            }
        }
        for (String strat : edge_partition_starts) {
            for (String partition_field : edge_partition_fields) {
                for (String query : queries_pattern_matching_ldbc) {
                    PatternMatchingBenchmark.SetQueryString(query);
                    PatternMatchingBenchmark.SetPartStrat(strat);
                    PatternMatchingBenchmark.SetPartField(partition_field);
                    PatternMatchingBenchmark.main(args);
                }
            }
        }
    }

    private static void runSnapshotBenchmark(String[] args) throws Exception{
        ArrayList<String> stringList = new ArrayList<>(Arrays.asList(args));
        stringList.add("-y");
        stringList.add("all");
        args = new String [stringList.size()];
        args = stringList.toArray(args);

        for (String strat : vertex_partition_starts) {
            for (String partition_field : vertex_partition_fields) {
                SnapshotBenchmark.SetPartStrat(strat);
                SnapshotBenchmark.SetPartField(partition_field);
                SnapshotBenchmark.main(args);
            }
        }

        for (String strat : edge_partition_starts) {
            for (String partition_field : edge_partition_fields) {
                SnapshotBenchmark.SetPartStrat(strat);
                SnapshotBenchmark.SetPartField(partition_field);
                SnapshotBenchmark.main(args);
            }
        }
    }
}
