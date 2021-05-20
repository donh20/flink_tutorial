package com.ncamc.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import java.io.Serializable;

//批处理wordcount程序
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "E:\\workspace\\flink\\src\\main\\resources\\hello.txt";
        DataSet<String> inpuDataSet = env.readTextFile(inputPath);

        //对数据集进行处理，按空格分词展开，转换成(word,1)这样的二元组进行统计
        /*
        public <R> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
            if (flatMapper == null) {
                throw new NullPointerException("FlatMap function must not be null.");
            } else {
                String callLocation = Utils.getCallLocationName();
                TypeInformation<R> resultType = TypeExtractor.getFlatMapReturnTypes(flatMapper, this.getType(), callLocation, true);
                return new FlatMapOperator(this, resultType, (FlatMapFunction)this.clean(flatMapper), callLocation);
            }
        }
        */
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T var1, Collector<O> var2) throws Exception;
        }
        */
        /*
        * flatMap可以让一条数据输出多个，api调用要传入的是flatmapFunction接口
        * 这个接口里面实现的是flatmap方法，无返回值，但是有Collector做为out
        * 收集器Collector对应的是iterator：
        * 迭代是一个一个迭代拿出来
        * Collector是收集，数据一个一个来，一个一个放进去
        * */
        DataSet<Tuple2<String, Integer>> resultSet = inpuDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)     //按照第一个位置的word分组
                .sum(1);          //讲第二个位置上的数据求和
        resultSet.print();
    }
    //自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {//注意不要引入scala的包

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按照空格分词
            String[] words = s.split(" ");

            //遍历所有的word,包成二元组输出
            for (String word:words) {
                //把(word,1)二元组收集起来
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
