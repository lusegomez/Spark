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

//            Dataset<Row> validQuads = validQuads(graphFrame, quadrilaterals);
//            validQuads.show();
//            String outputPath = saveResults(quadrilaterals, inputPath);
//            System.out.println("Results saved in: " + outputPath);

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

                    return !segmentsIntersect(A, B, C, D);
                })
                .select(
                        col("A.id").alias("vertexA"),
                        col("B.id").alias("vertexB"),
                        col("C.id").alias("vertexC"),
                        col("D.id").alias("vertexD")
                );

        return quads.orderBy("vertexA");
    }

    private static boolean segmentsIntersect(Row p1, Row q1, Row p2, Row q2) {
        //Segment 1
        int p1x = p1.getAs("x");;
        int p1y = p1.getAs("y");
        int q1x = q1.getAs("x");
        int q1y = q1.getAs("y");

        //Segment 2
        int p2x = p2.getAs("x");
        int p2y = p2.getAs("y");
        int q2x = q2.getAs("x");
        int q2y = q2.getAs("y");

        int o1 = orientation(p1x, p1y, q1x, q1y, p2x, p2y);
        int o2 = orientation(p1x, p1y, q1x, q1y, q2x, q2y);
        int o3 = orientation(p2x, p2y, q2x, q2y, p1x, p1y);
        int o4 = orientation(p2x, p2y, q2x, q2y, q1x, q1y);

        return o1 != o2 && o3 != o4;
    }

    public static int orientation(double x1, double y1, double x2, double y2, double x3, double y3) {
        double valor = (y2 - y1) * (x3 - x2) - (x2 - x1) * (y3 - y2);
        if (valor == 0) {
            return 0;
        }
        return (valor > 0) ? 1 : -1;
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

}
