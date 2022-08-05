# Gradoop Benchmarks
Dieses Repository basiert auf dem Repository [gradoop-Benchmark](https://github.com/dbs-leipzig/gradoop-benchmarks) der 
Universität Leipzig und wurde um verschiedene Partitionierungsmethoden für verschiedene Graphdatensätze erweitert.

Die Benchmarks 
- tpgm.PatternMathingBenchmark
- tpgm.SnapshotBenchmark

wurde um verschiedene Partitionierungsverfahren erweitertet, die im folgenden erläutert werden. Zusätzlich dazu wird 
gezeigt wie die verschiedenen Verfahren aufzurufen sind. 
Die verwendeten Graphdatensätze sind Citibike und ein LDBC-Social Network Benchmark.

## Partitioning

Das Repository umfasst die folgenden Partitionierungsverfahren:
- Hash
- Range
- edgeHash
- edgeRange
- DBH (Degree Based Hash)

Für alle Verfahren, bis auf DBH, können bestimmte Attribute als Partitionierungsattribut ausgewählt werden.
Für den Citibike-Datensatz sind für die Knoten des Graphen die Attribute
- id
- name
- regionId

und für die Kanten des Graphen die Attribute 
- id
- gender
- bike_id

verfügbar. Für den LDBC-Social Network Datensatz sind für die Knoten die Attribute
- id
- name

und für die Kanten das Attribut 
- id

verfügbar. Um die verschiedenen Partitionierungsstrategien und die Felder beim Aufruf des benchmarks zu definieren, gibt es zusätzliche Run-Parameter.
Diese sind in folgender Tabelle aufgelistet und erklärt.

| Parameter | Beschreibung                                                                                                                                                                                                                                                                                                                           |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -x        | Gibt die Partitionierungsstrategie an.                                                                                                                                                                                                                                                                                                 |
| -w        | Gibt das Partitionierungsattribut an.                                                                                                                                                                                                                                                                                                  |
| -a        | Gibt an, ob für jede Kante der Grad des Start- und des Endknoten berechnet werden soll. Die Grade werden als Properties an den Kanten gespeichert. Dies ist notwendig für das Partitionierungsverfahren DBH.                                                                                                                           |
| -z        | Gibt das PatternMatching (Cypher Query) an, die beim PatternMatching verwendet werden soll. Hier sind 6 verschiedene vordefineirte Cypher Queries hinterlegt. Diese können über den Index (1-6) ausgewählt werden. Dabei sind die Queries 1 bis 3 für den Datensatz Citibike und 4 bis 6 für den LDBC-Datensatz vorgesehen.            |
| -s        | Gibt an, ob der Graph, der um die Grade des Start- und Endknoten erweitert wurde, unter dem Output-Pfad des Standard Gradoop-Benchmarks gespeichert werden soll. (Dies ist nur relevant, wenn man den Graphen einmal vorberechnen möchte und anschließend auf Basis des vorberechneten Graphen den Benchmark erneut ausführen möchte.) |

Die Cypher Queries für den parameter -s sind wie folgt definiert:

| Index | Cypher Query |
|:-----:|--------------|
|   1   | MATCH (s1:station)-[t:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE t.bike_id = t2.bike_id       |
|   2   | MATCH (s1:station)-[t1:trip]->(s2:station)-[t2:trip]->(s3:station) WHERE s1.id = s2.id       |
|   3   | MATCH (v1:Station {cellId: 2883})-[t1:Trip]->(v2:Station)-[t2:Trip]->(v3:Station) WHERE v2.id != v1.id AND v2.id != v3.id AND v3.id != v1.id AND t1.val.precedes(t2.val) AND t1.val.lengthAtLeast(Minutes(30)) AND t2.val.lengthAtLeast(Minutes(30))       |
|   4   | MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post)       |
|   5   | MATCH (p:person)-[s:studyAt]->(u:university)       |
|   6   | MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) WHERE l.val_from.after(Timestamp(2012-06-01)) AND l.val_from.before(Timestamp(2012-06-02))       |

Im folgenden werden die Nutzung der verschiedenen Paramerter mit Beispielen aufgeziegt und das verhalten kurz erläutert. 

### Hash
Bei der Hash-Partitionierung werde Hash-Werte für die Attributwerte eines der zur Verfügung stehenden Attribute eines Knoten ermittelt.
Anhand dieser Hash-Werte, werden die Knoten auf die angegebenen Partitionen aufgeteilt.
  
Den SnpashotBenchmark kann man beispielsweise mit der Hash-Partitionierung und dem Attribut name wie folgt ausführen:

````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x hash -w name
````
bzw. für den PatternMatchingBenchmark
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -x hash -w name -s 1
````
Hierbei wird für die jeweiligen Attributwerte des Attributs "name" eine Hash-Partitionierung durchgeführt.

### Range
Bei der Range-Partitionierung werdedie Attributwerte eines der zur Verfügung stehenden Attribute eines Knoten in Intervalle unterteilt.
Die Intervalle bilden die Partitionen.

Den SnpashotBenchmark kann man beispielsweise mit der Range-Partitionierung und dem Attribut name wie folgt ausführen:

````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x range -w name
````
bzw. für den PatternMatchingBenchmark
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -x range -w name -s 1
````
Hierbei wird für die jeweiligen Attributwerte des Attributs "name" eine Range-Partitionierung durchgeführt.

### edgeHash
Bei der edgeHash-Partitionierung werde Hash-Werte für die Attributwerte eines der zur Verfügung stehenden Attribute eine Kante ermittelt.
Anhand dieser Hash-Werte, werden die Knoten auf die angegebenen Partitionen aufgeteilt.

Den SnpashotBenchmark kann man beispielsweise mit der edgeHash-Partitionierung und dem Attribut name wie folgt ausführen:

````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x edgeHash -w name
````
bzw. für den PatternMatchingBenchmark
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -x edgeHash -w name -s 1
````
Hierbei wird für die jeweiligen Attributwerte des Attributs "name" eine Hash-Partitionierung durchgeführt.

### edgeRange
Bei der edgeRange-Partitionierung werden die Attributwerte eines der zur Verfügung stehenden Attribute einer Kante in Intervalle unterteilt.
Die Intervalle bilden die Partitionen.

Den SnpashotBenchmark kann man beispielsweise mit der edgeRange-Partitionierung und dem Attribut name wie folgt ausführen:

````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x edgeRange -w name
````
bzw. für den PatternMatchingBenchmark
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -x edgeRange -w name -s 1
````
Hierbei wird für die jeweiligen Attributwerte des Attributs "name" eine Range-Partitionierung über die Kanten durchgeführt.

### DBH
Bei der DBH-Partitionierung werden vorab die Grade der Start- und Zielknoten einer Kante berechnet. Die Partitionierung verwendet dann die ID des Knoten mit dem größeren Grad gewählt, um die Partition festzulegen.
Die Partition wird ebenfalls über einen Hash-Wert festgelegt.
Hierbei ist zu beachten dass der parameter ```-a``` zwingend erforderlich ist, falls man keinen bereits vorberechneten Datensatz verwendet. 
Den SnpashotBenchmark kann man beispielsweise mit der DBH-Partitionierung und dem Attribut name wie folgt ausführen:

````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x DBH -a
````
bzw. für den PatternMatchingBenchmark
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.PatternMatchingBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -x DBH -s 1 -a
````

Wenn man die Vorberechnung der Knotengrade speichern möchte kann man den Aufruf um den Parameter ```-s``` erweitern.
````shell
 /opt/flink-1.9.0/bin/flink run -p 96 -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark /home/bddoop/kretschmer/gradoop-benchmarks-0.1.3.jar -o hdfs:///user/kretschmer/output -i hdfs:///user/kretschmer/citibike -c /home/bddoop/kretschmer/result.csv -n -y all -x DBH -a -z
````
Somit wird der Graph unter dem Output-Pfad ```-o``` (hdfs:///user/kretschmer/output) gespeichert.
Das Verzeichnis kann dann für einen erneuten Aufruf als Input verwendet werden.