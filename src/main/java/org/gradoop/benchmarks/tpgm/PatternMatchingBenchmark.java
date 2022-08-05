/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmarks.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated program to benchmark the query operator on temporal data.
 * The benchmark is expected to be executed on the LDBC data set.
 */
public class PatternMatchingBenchmark extends BaseTpgmBenchmark {
  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark
   * path/to/gradoop-benchmarks.jar -i hdfs:///graph -f indexed -o hdfs:///output -c results.csv}
   * <p>
   * It is advisable to use the {@link org.gradoop.temporal.io.impl.csv.indexed.TemporalIndexedCSVDataSource}
   * for a better performance by using parameter {@code -f indexed}.
   *
   * @param args program arguments
   * @throws Exception in case of an error
   */

  /**
   * Used query string indexed from 1..6.
  */
  private static String QUERY_STRING;
  /**
   * Option to select a predefined cypher query per Index for the datasets citibike (1,2,3) or ldbc (4,5,6)
  */
  private static final String QUERY_STRING_PARA = "z";

  static {
    OPTIONS.addOption(QUERY_STRING_PARA, "query", true, "Query String for Pattern Matching indexed per numbers from 1 to 6");
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, PatternMatchingBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readBaseCMDArguments(cmd);
    readCMDArguments(cmd);
    TemporalGraph graph = readTemporalGraph(INPUT_PATH, INPUT_FORMAT);
    ExecutionEnvironment env = graph.getConfig().getExecutionEnvironment();
    TemporalGradoopConfig conf = TemporalGradoopConfig.createConfig(env);

    //define several query strings selectable per run parameter -z. (1,2,3 = prepared for the dataset citibike, 4,5,6 prepared for the dataset ldbc)
    String temp_query = "";
    switch (QUERY_STRING){
      case "1":
      temp_query = "MATCH (s1:station)-[t:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE t.bike_id = t2.bike_id";
        break;
      case "2":
      temp_query = "MATCH (s1:station)-[t1:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE s1.id = s2.id";
        break;
      case "3":
      temp_query = "MATCH (v1:Station {cellId: 2883})-[t1:Trip]->(v2:Station)-[t2:Trip]->(v3:Station) WHERE v2.id != v1.id AND v2.id != v3.id AND v3.id != v1.id AND t1.val.precedes(t2.val) AND t1.val.lengthAtLeast(Minutes(30)) AND t2.val.lengthAtLeast(Minutes(30))";
        break;
      case "4":
      temp_query = "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post)";
        break;
      case "5":
      temp_query = "MATCH (p:person)-[s:studyAt]->(u:university)";
        break;
      case "6":
      temp_query = "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) WHERE l.val_from.after(Timestamp(2012-06-01)) AND l.val_from.before(Timestamp(2012-06-02))";
        break;
    }

    String query = temp_query;

    // calculate source and target degree of the vertices per edge
    if (CALC_DEGREE) {
      graph = CalculateSourceAndTargetDegrees(conf, graph);
    }
    if (SAVE_GRAPH) {
      graph.writeTo(new TemporalCSVDataSink(OUTPUT_PATH, conf), true);
    }

    //partition the graph for the selected partition strategy and the partition field
    if (PART_FIELD == null) {
      PART_FIELD = "id";
    }
    graph = PartitionGraph(conf, graph);
    
    TemporalGraphCollection results = graph.temporalQuery(query);

    // only count the results and write it to a csv file
    DataSet<Tuple2<String, Long>> sum = results.getGraphHeads()
      .map(g -> new Tuple2<>("G", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
      // group by the element type (V or E)
      .groupBy(0)
      // sum the values
      .sum(1);

    sum.writeAsCsv(appendSeparator(OUTPUT_PATH) + "count.csv", FileSystem.WriteMode.OVERWRITE);

    env.execute(PatternMatchingBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
        "Parallelism",
        "dataset",
        "query-type",
        "from(ms)",
        "to(ms)",
        "verify",
        "count-only",
        "Runtime(s)",
        "Part.-Strat.",
        "Partitioned Field",
        "Type",
        "Query");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
        env.getParallelism(),
        INPUT_PATH,
        null,
        null,
        null,
        null,
        COUNT_RESULT,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS),
        PARTITION_STRAT,
        PART_FIELD,
        "PatternMatching",
        QUERY_STRING);
    writeToCSVFile(head, tail);
  }

  private static void readCMDArguments(CommandLine cmd) {
    QUERY_STRING   = cmd.getOptionValue(QUERY_STRING_PARA);
  }
}
