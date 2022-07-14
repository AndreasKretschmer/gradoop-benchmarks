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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
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

  private static final String OPTION_PARTITION_STRAT = "p";
  private static final String OPTION_PARTITION_FIELD = "pf";
  private static String PARTITION_STRAT;
  private static String PART_FIELD;
  private static String QUERY_STRING;
  public static void SetPartStrat(String partStrat){
    PARTITION_STRAT = partStrat;
  }

  public static void SetPartField(String partField){
    PART_FIELD = partField;
  }
  public static void SetQueryString(String queryString) {QUERY_STRING = queryString;}
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

    String query = "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) " +
            "WHERE l.val_from.after(Timestamp(2012-06-01)) AND " +
            "      l.val_from.before(Timestamp(2012-06-02)) AND " +
            "      c.val_from.after(Timestamp(2012-05-30)) AND " +
            "      c.val_from.before(Timestamp(2012-06-02)) AND " +
            "      po.val_from.after(Timestamp(2012-05-30)) AND " +
            "      po.val_from.before(Timestamp(2012-06-02))";
    if (QUERY_STRING != null){
      query = QUERY_STRING;
    }

    if (PART_FIELD == null) {
      PART_FIELD = "id";
    }

    DataSet<WithCount<GradoopId>> vertexDegreeDataSet = new VertexDegrees().execute(graph.toLogicalGraph()); //List with the Format(GraphId, degree)
    DataSet<TemporalVertex> vertexes = graph.getVertices();
    DataSet<TemporalEdge> edges = graph.getEdges();
    edges = edges.join(vertexDegreeDataSet).where(v -> v.getSourceId()).equalTo(0).with(new JoinFunction<TemporalEdge, WithCount<GradoopId>, TemporalEdge>() {
      @Override
      public TemporalEdge join(TemporalEdge first, WithCount<GradoopId> second) throws Exception {
        first.setProperty("SourceDegree", second.f1);
        return first;
      }
    });
    edges = edges.join(vertexDegreeDataSet).where(v -> v.getTargetId()).equalTo(0).with(new JoinFunction<TemporalEdge, WithCount<GradoopId>, TemporalEdge>() {
      @Override
      public TemporalEdge join(TemporalEdge first, WithCount<GradoopId> second) throws Exception {
        first.setProperty("TargetDegree", second.f1);
        return first;
      }
    });

    graph = conf.getTemporalGraphFactory().fromDataSets( graph.getGraphHead(), vertexes , edges);
    //graph vorverarbeiten 1Mal -> mehrfach (Präsentation)

    final String finalPartField = PART_FIELD;
    final String finalPartStrat = PARTITION_STRAT;

    switch (finalPartStrat) {
      case "hash": {
        switch (finalPartField) {
          case "id":
            vertexes = graph.getVertices().partitionByHash(finalPartField);
            break;
          default:
            vertexes = graph.getVertices().partitionByHash(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                return value.getPropertyValue(finalPartField).toString();
              }
            });
            break;
        }
        break;
      }
      case "edgeHash": {
        switch (finalPartField) {
          case "id":
            edges = graph.getEdges().partitionByHash(finalPartField);
            break;
          default:
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                return value.getPropertyValue(finalPartField).toString();
              }
            });
            break;
        }
        break;
      }
      case "range":{
        switch (finalPartField) {
          case "id":
            vertexes = graph.getVertices().partitionByRange(finalPartField);
            break;
          default:
            vertexes = graph.getVertices().partitionByRange(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                return value.getPropertyValue(finalPartField).toString();
              }
            });
            break;
        }
        break;
      }
      case "edgeRange":{
        switch (finalPartField) {
          case "id":
            edges = graph.getEdges().partitionByRange(finalPartField);
            break;
          default:
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                return value.getPropertyValue(finalPartField).toString();
              }
            });
            break;
        }
        break;
      }
      case "DBH":
        vertexes = graph.getVertices().partitionCustom(new Partitioner<GradoopId>() {
          @Override
          public int partition(GradoopId key, int numPartitions) {
            return key.hashCode() % numPartitions;
          }
        }, new Id<>());
        edges = graph.getEdges().partitionCustom(new Partitioner<GradoopId>() {
          @Override
          public int partition(GradoopId key, int numPartitions) {
            return key.hashCode() % numPartitions;
          }
        }, new KeySelector<TemporalEdge, GradoopId>() {
          @Override
          public GradoopId getKey(TemporalEdge value) throws Exception {
            PropertyValue SourceDegree = value.getPropertyValue("SourceDegree");
            PropertyValue TargetDegree = value.getPropertyValue("TargetDegree");

            if (SourceDegree.getLong() > TargetDegree.getLong()) {
              return value.getSourceId();
            }
            else{
              return value.getTargetId();
            }
          }
        });
        break;
      default:
        break;
    }
    graph = conf.getTemporalGraphFactory().fromDataSets( graph.getGraphHead(),vertexes , edges);
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
      .format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
        "Parallelism",
        "dataset",
        "query-type",
        "from(ms)",
        "to(ms)",
        "verify",
        "count-only",
        "Runtime(s)",
        "Part.-Strat.",
        "Partitioned Field");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
        env.getParallelism(),
        INPUT_PATH,
        null,
        null,
        null,
        null,
        COUNT_RESULT,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS),
        PARTITION_STRAT,
        PART_FIELD);
    writeToCSVFile(head, tail);
  }

  private static void readCMDArguments(CommandLine cmd) {
    PARTITION_STRAT = cmd.getOptionValue(OPTION_PARTITION_STRAT);
    PART_FIELD = cmd.getOptionValue(OPTION_PARTITION_FIELD);
  }
}
