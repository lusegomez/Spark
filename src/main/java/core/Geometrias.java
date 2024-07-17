package core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.graphframes.GraphFrame;

import javax.xml.crypto.Data;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

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
            dataset.show();

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
            vertices.show();

            Dataset<Row> edges = dataset.selectExpr("id1 as src", "id2 as dst");
            Dataset<Row> reversedEdges = edges.selectExpr("dst as src", "src as dst");
            edges = edges.union(reversedEdges).dropDuplicates();
            edges.show();

            GraphFrame graphFrame = new GraphFrame(vertices, edges);

            Dataset<Row> quads = findQuads(graphFrame);
            quads.show();


            String outputPath = saveResults(quads, inputPath);
            System.out.println("Results saved in: " + outputPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkContext.close();
            sparkSession.close();
        }
    }

    private static Column cantorPairing(Column x, Column y) {
        // Formula modificada: (x^2 + x + 2xy + 3y + y^2) / 2
        return expr("(((" + x + " + " + y + ") / 2) * ((" + x + " + " + y + ") + 1)) + " + y);
    }

    private static Dataset<Row> findQuads(GraphFrame graphFrame) {
        Dataset<Row> quads = graphFrame.find("(A)-[ab]->(B); (B)-[bc]->(C); (C)-[cd]->(D); (D)-[da]->(A)")
                .filter("A != B AND A != C AND A != D AND B != C AND B != D AND C != D")
                .filter(row -> {
                    Row A = row.getAs("A");
                    Row B = row.getAs("B");
                    Row C = row.getAs("C");
                    Row D = row.getAs("D");

                    return !segmentsIntersect(A, B, C, D) && !segmentsIntersect(B, C, D, A);
                })
                .filter(row -> {
                    Row A = row.getAs("A");
                    Row B = row.getAs("B");
                    Row C = row.getAs("C");
                    Row D = row.getAs("D");

                    return antiClockwise(A,B,C,D);
                })
                .withColumn("sorted_ids", sort_array(array(col("A.id"), col("B.id"), col("C.id"), col("D.id"))))
                .orderBy("A.id")
                .dropDuplicates("sorted_ids")
                .select(
                        col("A.id").alias("id1"),
                        col("A.x").alias("x1"),
                        col("A.y").alias("y1"),
                        col("B.id").alias("id2"),
                        col("B.x").alias("x2"),
                        col("B.y").alias("y2"),
                        col("C.id").alias("id3"),
                        col("C.x").alias("x3"),
                        col("C.y").alias("y3"),
                        col("D.id").alias("id4"),
                        col("D.x").alias("x4"),
                        col("D.y").alias("y4"),
                        col("A.id").alias("id5"),
                        col("A.x").alias("x5"),
                        col("A.y").alias("y5")
                );
        return quads.orderBy("id1");
    }

    private static boolean antiClockwise(Row A, Row B, Row C, Row D) {
        return orientation(A, B, C) == -1 &&
                orientation(B, C, D) == -1 &&
                orientation(C, D, A) == -1;
    }

    private static boolean segmentsIntersect(Row A, Row B, Row C, Row D) {
        int o1 = orientation(A, B, C);
        int o2 = orientation(A, B, D);
        int o3 = orientation(C, D, A);
        int o4 = orientation(C, D, B);

        return o1 != o2 && o3 != o4;
    }

    public static int orientation(Row p, Row q, Row r) {
        int px = p.getAs("x");
        int py = p.getAs("y");
        int qx = q.getAs("x");
        int qy = q.getAs("y");
        int rx = r.getAs("x");
        int ry = r.getAs("y");

        double val = (qy - py) * (rx - qx) - (qx - px) * (ry - qy);
        if (val == 0) {
            return 0;
        }
        return (val > 0) ? 1 : -1;
    }

    private static String saveResults(Dataset<Row> results, String inputPath) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());

        String directory = new File(inputPath).getParent() + "/" + timestamp;

        results.write()
            .option("header", "true")
            .csv(directory);
     return directory;
    }

}
