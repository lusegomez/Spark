package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.spark.sql.functions.*;

public class Geometrias {
    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: SparkGeometry <input csv path>");
            System.exit(1);
        }
        String inputPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Geometrias");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder()
                .sparkContext(sparkContext.sc())
                .getOrCreate();

        try {
            Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(inputPath);

            dataset = dataset.select(
                    dataset.col("x1").cast("int"),
                    dataset.col("y1").cast("int"),
                    dataset.col("x2").cast("int"),
                    dataset.col("y2").cast("int")
            ).dropDuplicates();

            // Remove self-loops
            dataset = dataset.filter(
                    dataset.col("x1").notEqual(dataset.col("x2"))
                            .or(dataset.col("y1").notEqual(dataset.col("y2")))
            );

            //Drop negative coords
            dataset = dataset.filter(
                    dataset.col("x1").geq(0).and(dataset.col("y1").geq(0))
                            .and(dataset.col("x2").geq(0)).and(dataset.col("y2").geq(0))
            );

            // Cantor pairing
            dataset = dataset.withColumn("id1", cantorPairing(dataset.col("x1"), dataset.col("y1")))
                    .withColumn("id2", cantorPairing(dataset.col("x2"), dataset.col("y2")));

            Dataset<Row> vertices1 = dataset.select(
                    dataset.col("id1").alias("id"),
                    dataset.col("x1").alias("x"),
                    dataset.col("y1").alias("y")
            );
            Dataset<Row> vertices2 = dataset.select(
                    dataset.col("id2").alias("id"),
                    dataset.col("x2").alias("x"),
                    dataset.col("y2").alias("y")
            );

            Dataset<Row> vertices = vertices1.union(vertices2).distinct();

            Dataset<Row> edges = dataset.selectExpr("id1 as src", "id2 as dst");
            edges = edges.withColumn("sorted_src", when(col("src").lt(col("dst")), col("src")).otherwise(col("dst")))
                    .withColumn("sorted_dst", when(col("src").lt(col("dst")), col("dst")).otherwise(col("src")))
                    .dropDuplicates("sorted_src", "sorted_dst")
                    .drop("sorted_src", "sorted_dst");
            dataset.show();
            vertices.show();
            edges.show();
            GraphFrame graphFrame = new GraphFrame(vertices, edges);
            long numVertices = graphFrame.vertices().count();
            long numEdges = graphFrame.edges().count();
            System.out.println("Node qty: " + numVertices);
            System.out.println("Edge qty: " + numEdges);

            Dataset<Row> quadrilaterals = findQuads(graphFrame);
            quadrilaterals.show();

//            String outputPath = saveResults(quadrilaterals, inputPath);
//            System.out.println("Results saved in: " + outputPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkContext.close();
            sparkSession.close();
        }
    }

    private static Dataset<Row> findQuads(GraphFrame graphFrame) {
        Dataset<Row> quads = graphFrame.find("(A)-[ab]->(B); (B)-[bc]->(C); (C)-[cd]->(D); (D)-[da]->(A)");
                .where("A.id < B.id AND B.id < C.id AND C.id < D.id")
                .select(
                        col("A.id").alias("vertexA"),
                        col("B.id").alias("vertexB"),
                        col("C.id").alias("vertexC"),
                        col("D.id").alias("vertexD")
                );

        return quads.orderBy("vertexA");
        return quads;
    }

    private static String saveResults(Dataset<Row> results, String inputPath) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String outputPath = inputPath.replace(".csv", "_" + timestamp + ".csv");
        System.out.println("Contenido del DataFrame 'results':");
        results.show();
        results.coalesce(1)
                .write()
                .option("header", "true")
                .csv(outputPath);

        // Rename the output file to part-00000.csv
        File outputFile = new File(outputPath);
        File[] files = outputFile.listFiles((dir, name) -> name.startsWith("part-") && name.endsWith(".csv"));
        if (files != null && files.length > 0) {
            File renamedFile = new File(outputFile.getParent(), "part-" + timestamp + ".csv");
            files[0].renameTo(renamedFile);
            return renamedFile.getAbsolutePath();
        }

        return outputPath;
    }
//
//    private static org.apache.spark.sql.Column cantorPairing(org.apache.spark.sql.Column x, org.apache.spark.sql.Column y) {
//        return functions.expr("cast(((" + x + " * (" + x + " + " + y + " + 1)) / 2) + " + y + " as int)");
//    }

    private static Column cantorPairing(Column x, Column y) {
        // Formula modificada: (x^2 + x + 2xy + 3y + y^2) / 2
        return expr("(((" + x + " + " + y + ") / 2) * ((" + x + " + " + y + ") + 1)) + " + y);
    }
}
