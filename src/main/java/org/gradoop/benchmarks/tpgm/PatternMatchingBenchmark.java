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
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

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
  private static String PARTITION_STRAT;
  private static String PART_FIELD;
  
  public static void SetPartStrat(String partStrat){
    PARTITION_STRAT = partStrat;
  }

  public static void SetPartField(String partField){
    PART_FIELD = partField;
  }
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, PatternMatchingBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readBaseCMDArguments(cmd);

    TemporalGraph graph = readTemporalGraph(INPUT_PATH, INPUT_FORMAT);

    ExecutionEnvironment env = graph.getConfig().getExecutionEnvironment();

    String query = "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) " +
      "WHERE l.val_from.after(Timestamp(2012-06-01)) AND " +
      "      l.val_from.before(Timestamp(2012-06-02)) AND " +
      "      c.val_from.after(Timestamp(2012-05-30)) AND " +
      "      c.val_from.before(Timestamp(2012-06-02)) AND " +
      "      po.val_from.after(Timestamp(2012-05-30)) AND " +
      "      po.val_from.before(Timestamp(2012-06-02))";

    switch (PARTITION_STRAT) {
      case "hash":
        graph.getVertices().partitionByHash(PART_FIELD);
        break;
      case "edgeHash":
        graph.getEdges().partitionByHash(PART_FIELD); //Edge-Cut
        break;
      case "range":
        graph.getVertices().partitionByRange(PART_FIELD);
        break;
      case "edgeRange":
        graph.getEdges().partitionByRange(PART_FIELD);
        break;
      case "DBH":
        graph.getEdges().partitionCustom(new DBH(), PART_FIELD);
        break;
      case "LDG":
        graph.getEdges().partitionCustom(new LDG(), PART_FIELD);
        break;
      default:
        break;
    }

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
    String head = String.format("%s|%s|%s|%s|%s|%s", "Parallelism", "dataset", "format", "Runtime(s)", "Partition Strategy", "Partitioned Field");
    String tail = String.format("%s|%s|%s|%s|%s|%s", env.getParallelism(), INPUT_PATH, INPUT_FORMAT,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS),
      PARTITION_STRAT,
      PART_FIELD);
    writeToCSVFile(head, tail);
  }
}
