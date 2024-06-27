package core;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Geometrias {
    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: SparkGeometry <input csv path>");
            System.exit(1);
        }
        String inputPath = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("Geometrias").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        try {
            Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(inputPath);

            dataset = dataset.select(
                    dataset.col("x1").cast("int"),
                    dataset.col("y1").cast("int"),
                    dataset.col("x2").cast("int"),
                    dataset.col("y2").cast("int")
            );
            dataset = dataset.dropDuplicates();

            //Remove self-loops
            dataset = dataset.filter(dataset.col("x1").notEqual(dataset.col("x2")).or(dataset.col("y1").notEqual(dataset.col("y2"))));

            System.out.println("Dataset:" );
            dataset.show();

            dataset = dataset.filter(dataset.col("x1").geq(0).and(dataset.col("y1").geq(0)))
                    .filter(dataset.col("x2").geq(0).and(dataset.col("y2").geq(0)))
                    .withColumn("id1", cantorPairing(dataset.col("x1"), dataset.col("y1")))
                    .withColumn("id2", cantorPairing(dataset.col("x2"), dataset.col("y2")));

            Dataset<Row> vertices = dataset.selectExpr("id1 as id").union(dataset.selectExpr("id2 as id")).distinct();
            System.out.println("Vertices:" );
            vertices.show();
            Dataset<Row> edges = dataset.selectExpr("id1 as src", "id2 as dst");
            System.out.println("Edges:" );
            edges.show();

            GraphFrame graphFrame = new GraphFrame(vertices, edges);
            
            Dataset<Row> quadrilaterals = findQuads(graphFrame);
            String outputPath = saveResults(quadrilaterals, inputPath);

            System.out.println("Resultados guardados en: " + outputPath);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkContext.close();
            sparkSession.close();
        }

    }

    private static Dataset<Row> findQuads(GraphFrame graphFrame) {
        return graphFrame.find("(A)-[ab]->(B); (A)-[ad]->(D); (B)-[bc]->(C); (D)-[cd]->(C)")
                .filter("A.id < B.id AND B.id < C.id AND C.id < D.id");
    }

    private static String saveResults(Dataset<Row> results, String inputPath) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = sdf.format(new Date());
        String outputPath = inputPath.replace(".csv", "_" + timestamp + ".csv");

        results.coalesce(1)  // Para guardar en un solo archivo
                .write()
                .option("header", "true")
                .csv(outputPath);

        return outputPath;
    }
    private static org.apache.spark.sql.Column cantorPairing(org.apache.spark.sql.Column x, org.apache.spark.sql.Column y) {
        return x.multiply(x.plus(y).plus(1)).divide(2).plus(y);
    }
}
