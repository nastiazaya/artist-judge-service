import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class MusicController {
    @Autowired
    private JavaSparkContext sc;
    @GetMapping
    public List<String> topX(String pathToData, int x) {
        //todo finish it
        JavaRDD<String> stringJavaRDD = sc.textFile(pathToData);
        return stringJavaRDD.flatMap(string -> Arrays.asList(string.split(" "))
                .iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b).take(x)
                .stream()
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
    }
}
