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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.benchmarks.AbstractRunner;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegrees;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Base class for the TPGM benchmarks.
 */
abstract class BaseTpgmBenchmark extends AbstractRunner {
  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare the graph input format (csv or indexed).
   */
  private static final String OPTION_INPUT_FORMAT = "f";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to csv file with execution results
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to count the result sets instead of writing them
   */
  private static final String OPTION_COUNT_RESULT = "n";
  /**
   * Option to select a custom partition strategy (hash, range, edgeHash, edgeRange, DBH)
   */
  private static final String OPTION_PARTITION_STRAT = "x";
  /**
   * Option to select a custom partition field for the given partition strategy (e.g. name)
   */
  private static final String OPTION_PARTITION_FIELD = "w";
  /**
   * Option to calculate an in- and outdegree per edge  and save the new graph to the output path (it is necessary to load a graph with these calculated degrees for the DBH partition strategy) if this parameter is not given u have to load a graph with the properties needed
   */
  private static final String OPTION_CALC_DEGREE = "a";
  /**
   * Option to define whether the new calculated degrees should be saved to a file
   */
  private static final String OPTION_SAVE_GRAPH = "s";

  /**
   * Used input path
   */
  static String INPUT_PATH;
  /**
   * Used input format (csv or indexed)
   */
  static String INPUT_FORMAT;
  /**
   * Used output path
   */
  static String OUTPUT_PATH;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used count only flag. The graph elements will be counted only if this is set to true.
   */
  static boolean COUNT_RESULT;
  /**
   * Used calculate in- and outdegrees per edgge flag. Degrees will be calculated only if this is set to true.
   */
  static boolean CALC_DEGREE;
  /**
   * Used to define whether the graph should be saved to a file or not.
   */
  static boolean SAVE_GRAPH;
  /**
   * Used partition strategy.
   */
  static String PARTITION_STRAT;
  /**
   * Used partition field.
   */
  static String PART_FIELD;
  /**
   * Broadcastvariable to partition Dataset by name. Is used for PART_FIELD=name.
   */
  static final String partitionField_name = "name";
  /**
   * Broadcastvariable to partition Dataset by regionId. Is used for PART_FIELD=regionId.
   */
  static final String partitionField_regionId = "regionId";
  /**
   * Broadcastvariable to partition Dataset by gender. Is used for PART_FIELD=gender.
   */
  static final String partitionField_gender = "gender";
  /**
   * Broadcastvariable to partition Dataset by bike_id. Is used for PART_FIELD=bike_id.
   */
  static final String partitionField_bikeid = "bike_id";

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to source files.");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true, "Input graph format (csv (default), indexed).");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output file.");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv result file (will be created if not available).");
    OPTIONS.addOption(OPTION_COUNT_RESULT, "count", false, "Only count the results instead of writing them.");
    OPTIONS.addOption(OPTION_PARTITION_STRAT, "partStrat", true, "Used partition strategy");
    OPTIONS.addOption(OPTION_PARTITION_FIELD, "partField", true, "Used partition field");
    OPTIONS.addOption(OPTION_CALC_DEGREE, "CalcDegree", false, "Used to define, if degree should be calculated");
    OPTIONS.addOption(OPTION_SAVE_GRAPH, "Save Graph", false, "Used to define, whether the new calculated graph should be saved");
  }

  /**
   * A function to output the results. There are two ways according to the flag COUNT_RESULT.
   * <p>
   * If it is set to TRUE, the vertices and edges of the graph will be counted and the result will be written
   * in a file named 'count.csv' inside the given output directory.
   * <p>
   * If it it set to FALSE, the whole graph will be written in the output directory by the
   * {@link TemporalCSVDataSink}.
   *
   * @param temporalGraph the temporal graph to write
   * @param conf the temporal gradoop config
   * @throws IOException if writing the result fails
   */
  static void writeOrCountGraph(TemporalGraph temporalGraph, GradoopFlinkConfig conf) throws IOException {
    if (COUNT_RESULT) {
      // only count the results and write it to a csv file
      DataSet<Tuple2<String, Long>> sum = temporalGraph.getVertices()
        .map(v -> new Tuple2<>("V", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
        .union(temporalGraph.getEdges()
          .map(e -> new Tuple2<>("E", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {}))
        // group by the element type (V or E)
        .groupBy(0)
        // sum the values
        .sum(1);

      sum.writeAsCsv(appendSeparator(OUTPUT_PATH) + "count.csv", FileSystem.WriteMode.OVERWRITE);
    } else {
      // write graph to sink
      TemporalDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, conf);
      sink.write(temporalGraph, true);
    }
  }

  /**
   * Reads main arguments (input path, output path, csv path and count flag) from command line.
   *
   * @param cmd command line
   */
  static void readBaseCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT, DEFAULT_FORMAT);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
    COUNT_RESULT = cmd.hasOption(OPTION_COUNT_RESULT);
    CALC_DEGREE = cmd.hasOption(OPTION_CALC_DEGREE);
    PARTITION_STRAT = cmd.getOptionValue(OPTION_PARTITION_STRAT);
    PART_FIELD = cmd.getOptionValue(OPTION_PARTITION_FIELD);
    SAVE_GRAPH = cmd.hasOption(OPTION_SAVE_GRAPH);
  }

  /**
   * Writes the specified line (csvTail) to the csv file given by option {@value OPTION_CSV_PATH}.
   * If the file does not exist, the file will be created and the specified header (csvHead) will be appended
   * as first line.
   *
   * @param csvHead the header (i.e., column names) for the csv file
   * @param csvTail the line to append
   * @throws IOException in case of a IO failure
   */
  static void writeToCSVFile(String csvHead, String csvTail) throws IOException {
    Path path = Paths.get(CSV_PATH);
    List<String> linesToWrite;
    if (Files.exists(path)) {
      linesToWrite = Collections.singletonList(csvTail);
    } else {
      linesToWrite = Arrays.asList(csvHead, csvTail);
    }
    Files.write(path, linesToWrite, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
      StandardOpenOption.APPEND);
  }

  /**
   * Calculates the degree of the source and target vertex of an edge.
   * The calculated degrees gets saved as properties (SourceDegree, TargetDegree) of the edge.
   *
   * @param conf the temporal gradoop config
   * @param inputGraph the temporal graph to calculate and write the degrees to
   * @return returns a new temporal graph with the calculated degrees per edge
   * @throws Exception if an error occures while calculating the degrees
   */
  static TemporalGraph CalculateSourceAndTargetDegrees(TemporalGradoopConfig conf, TemporalGraph inputGraph) throws Exception{
    DataSet<WithCount<GradoopId>> vertexDegreeDataSet = new VertexDegrees().execute(inputGraph.toLogicalGraph()); //List with the Format(GraphId, degree)
    DataSet<TemporalVertex> vertexes = inputGraph.getVertices();
    DataSet<TemporalEdge> edges = inputGraph.getEdges();
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

    // create new graph with the degrees as properties
    TemporalGraph graph = conf.getTemporalGraphFactory().fromDataSets(inputGraph.getGraphHead(), vertexes, edges);
    //save the new graph as a file in the output directory
    //graph.writeTo(new TemporalCSVDataSink(OUTPUT_PATH, conf));
    return graph;
  }

  /**
   * Partitions a given graph for a given partition strategy and a given partition field
   * The partition strategy and the partition field can be defined as run parameter (-x and -w)
   *
   * @param conf the temporal gradoop config
   * @param graph the temporal graph that should be partitioned
   * @return returns a new partitioned temporal graph
   * @throws Exception if an error occures while calculating the degrees
   */
  static TemporalGraph PartitionGraph(TemporalGradoopConfig conf, TemporalGraph graph) throws Exception {
    DataSet<TemporalVertex> vertexes = graph.getVertices();
    DataSet<TemporalEdge> edges = graph.getEdges();
    switch (PARTITION_STRAT) {
      //partitions the vertices for the given partition field per hash
      case "hash": {
        switch (PART_FIELD) {
          case "id":
            vertexes = graph.getVertices().partitionByHash(PART_FIELD);
            break;
          case "name":
            vertexes = graph.getVertices().partitionByHash(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                if (value.hasProperty(partitionField_name)) {
                  return value.getPropertyValue(partitionField_name).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "label":
            vertexes = graph.getVertices().partitionByHash(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                return(value.getLabel());
              }
            });
          case "regionId":
            vertexes = graph.getVertices().partitionByHash(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                if (value.hasProperty(partitionField_regionId)) {
                  return value.getPropertyValue(partitionField_regionId).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
        }
        break;
      }
      //partitions the edges for the given partition field per hash
      case "edgeHash": {
        switch (PART_FIELD) {
          case "id":
            edges = graph.getEdges().partitionByHash(PART_FIELD);
            break;
          case "name":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_name)) {
                  return value.getPropertyValue(partitionField_name).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "regionId":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_regionId)) {
                  return value.getPropertyValue(partitionField_regionId).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "gender":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_gender)) {
                  return value.getPropertyValue(partitionField_gender).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "bike_id":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_bikeid)) {
                  return value.getPropertyValue(partitionField_bikeid).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "day":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, Long>() {
              @Override
              public Long getKey(TemporalEdge value) throws Exception {
                return(value.getValidFrom() / (1000*60*60*24));
              }
            });
            break;
          case "year":
            edges = graph.getEdges().partitionByHash(new KeySelector<TemporalEdge, Integer>() {
              @Override
              public Integer getKey(TemporalEdge value) throws Exception {
                Integer year = LocalDateTime.ofEpochSecond(value.getValidFrom() / 1000, 0, ZoneOffset.UTC).getYear();
                return(year);
              }
            });
            break;
        }
        break;
      }
      //partitions the vertices for the given partition field per range
      case "range":{
        switch (PART_FIELD) {
          case "id":
            vertexes = graph.getVertices().partitionByRange(PART_FIELD);
            break;
          case "name":
            vertexes = graph.getVertices().partitionByRange(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                if (value.hasProperty(partitionField_name)) {
                  return value.getPropertyValue(partitionField_name).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "regionId":
            vertexes = graph.getVertices().partitionByRange(new KeySelector<TemporalVertex, String>() {
              @Override
              public String getKey(TemporalVertex value) throws Exception {
                if (value.hasProperty(partitionField_regionId)) {
                  return value.getPropertyValue(partitionField_regionId).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
        }
        break;
      }
      //partitions the edges for the given partition field per range
      case "edgeRange":{
        switch (PART_FIELD) {
          case "id":
            edges = graph.getEdges().partitionByRange(PART_FIELD);
            break;
          case "name":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_name)) {
                  return value.getPropertyValue(partitionField_name).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "regionId":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_regionId)) {
                  return value.getPropertyValue(partitionField_regionId).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "gender":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_gender)) {
                  return value.getPropertyValue(partitionField_gender).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "bike_id":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, String>() {
              @Override
              public String getKey(TemporalEdge value) throws Exception {
                if (value.hasProperty(partitionField_bikeid)) {
                  return value.getPropertyValue(partitionField_bikeid).toString();
                } else {
                  return "1";
                }
              }
            });
            break;
          case "day":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, Long>() {
              @Override
              public Long getKey(TemporalEdge value) throws Exception {
                return(value.getValidFrom() / (1000*60*60*24));
              }
            });
            break;
          case "year":
            edges = graph.getEdges().partitionByRange(new KeySelector<TemporalEdge, Integer>() {
              @Override
              public Integer getKey(TemporalEdge value) throws Exception {
                Integer year = LocalDateTime.ofEpochSecond(value.getValidFrom() / 1000, 0, ZoneOffset.UTC).getYear();
                return(year);
              }
            });
            break;
        }
        break;
      }
      //partitions the edges and vertices per Degree-Based Hashing (DBH)
      case "DBH": {
        vertexes = graph.getVertices().partitionCustom(new Partitioner<GradoopId>() {
          @Override
          public int partition(GradoopId key, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
          }
        }, new Id<>());
        edges = graph.getEdges().partitionCustom(new Partitioner<GradoopId>() {
          @Override
          public int partition(GradoopId key, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
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
      }
      default:
        break;
    }

    graph = conf.getTemporalGraphFactory().fromDataSets( graph.getGraphHead(),vertexes , edges);
    //save the partitioned graph

    return(graph);
  }
}
