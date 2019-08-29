package com.mypackage;

import com.mypackage.results.*;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;


public class Procedures {

    // 下面的行声明需要包括GraphDatabaseService类
    // 作为项目运行的环境(context)。
    @Context
    public GraphDatabaseService db;

    // 获得日志操作类的实例。
    // 标准日志通常是`data/log/neo4j.log`。
    @Context
    public Log log;

    @Procedure(name = "com.mypackage.hello", mode = Mode.READ)
    @Description("CALL com.mypackage.hello(String name)")
    public Stream<StringResult> hello(@Name("name") String name) {
        return Stream.of(new StringResult("Hello, " + name + "!"));
    }

    @UserFunction(name = "com.mypackage.greetings")
    @Description("com.mypackage.greetings(String name)")
    public String greetings(@Name("name") String name) {
        return new String("Hello, " + name + "!");
    }

    @UserAggregationFunction
    @Description( "com.mypackage.eldestAge(age) – 返回最大的年龄。" )
    public LongIntegerAggregator eldestAge ()
    {
        return new LongIntegerAggregator();
    }

    public static class LongIntegerAggregator
    {
        private Long eldest = 0l;

        @UserAggregationUpdate
        public void findEldest(
                @Name( "age" ) Long age )
        {
            if ( age > eldest)
            {
                eldest = age;
            }
        }

        @UserAggregationResult
        public Long result()
        {
            return eldest;
        }
    }

    // 统计k-度邻居的数量knn1：使用HashSet保存节点，方向无关
    // 参数：node - 起始节点, distance - 距离
    @Procedure(name = "com.mypackage.knn1.count", mode = Mode.READ)
    @Description("CALL com.mypackage.knn1.count(Node node, Long distnace)")
    public Stream<LongResult> neighbourCount1(@Name("node") Node node,
                                             @Name(value="distance", defaultValue = "1") Long distance)
            throws IOException
    {
        if (distance < 1) return Stream.empty();

        if (node == null) return Stream.empty();

        Iterator<Node> iterator;
        Node current;

        HashSet<Node> seen = new HashSet<>();   // 保存找到的节点
        HashSet<Node> nextA = new HashSet<>();  // 保存偶数层节点
        HashSet<Node> nextB = new HashSet<>();  // 保存奇数层节点

        // 处理起始节点
        seen.add(node);
        nextA.add(node);

        // 寻找第一层的节点 => nextB
        for (Relationship r : node.getRelationships()) {
            nextB.add(r.getOtherNode(node));
        }

        // 从第一层的节点开始，寻找下一层节点、直到到达distance层
        for (int i = 1; i < distance; i++) {
            // 这里处理偶数层：2、4、6、8...

            // 去除已经访问过的节点
            nextB.removeAll(seen);

            seen.addAll(nextB);
            nextA.clear();
            iterator = nextB.iterator();

            while (iterator.hasNext()) {
                current = iterator.next();
                for (Relationship r : current.getRelationships()) {
                    nextA.add(r.getOtherNode(current));
                }
            }

            i++;

            if (i < distance) {
                // 这里处理奇数层：3、5、7...
                nextA.removeAll(seen);
                seen.addAll(nextA);
                nextB.clear();
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    current = iterator.next();
                    for (Relationship r : current.getRelationships()) {
                        nextB.add(r.getOtherNode(current));
                    }
                }
            }
        }

        // 退出循环时，将最后一层的节点保存到已访问节点列表中
        if ((distance % 2) == 0) {
            nextA.removeAll(seen);
            seen.addAll(nextA);
        } else {
            nextB.removeAll(seen);
            seen.addAll(nextB);
        }

        // 去除起始节点
        seen.remove(node);
        return Stream.of(new LongResult((long) seen.size()));
    }

    // 统计k-度邻居的数量knn1：使用RoaringBitmap保存节点，方向无关
    // 参数：node - 起始节点, distance - 距离
    @Procedure(name = "com.mypackage.knn2.count", mode = Mode.READ)
    @Description("CALL com.mypackage.knn2.count(Node node, Long distnace)")
    public Stream<LongResult> neighbourCount2(@Name("node") Node node,
                                              @Name(value="distance", defaultValue = "1") Long distance)
            throws IOException
    {
        if (distance < 1) return Stream.empty();

        if (node == null) return Stream.empty();

        Iterator<Node> iterator;
        Node current;

        RoaringBitmap seen = new RoaringBitmap();   // 保存找到的节点
        RoaringBitmap nextA = new RoaringBitmap();  // 保存偶数层节点
        RoaringBitmap nextB = new RoaringBitmap();  // 保存奇数层节点

        int startNodeId = (int) node.getId();

        // 处理起始节点
        seen.add(startNodeId);
        nextA.add(startNodeId);

        // 寻找第一层的节点 => nextB
        for (Relationship r : node.getRelationships()) {
            nextB.add((int) r.getEndNodeId());
            nextB.add((int) r.getStartNodeId());
        }

        // 从第一层的节点开始，寻找下一层节点、直到到达distance层
        for (int i = 1; i < distance; i++) {
            // 这里处理偶数层：2、4、6、8...

            // 去除已经访问过的节点
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();

            for (Integer nodeId : nextB) {
                for (Relationship r : db.getNodeById((long) nodeId).getRelationships()) {
                    nextA.add((int) r.getEndNodeId());
                    nextA.add((int) r.getStartNodeId());
                }
            }

            i++;

            if (i < distance) {
                // 这里处理奇数层：3、5、7...
                nextA.andNot(seen);
                seen.or(nextA);
                nextB.clear();

                for (Integer nodeId : nextA) {
                    for (Relationship r : db.getNodeById((long) nodeId).getRelationships()) {
                        nextB.add((int) r.getEndNodeId());
                        nextB.add((int) r.getStartNodeId());
                    }
                }
            }
        }

        // 退出循环时，将最后一层的节点保存到已访问节点列表中
        if ((distance % 2) == 0) {
            nextA.andNot(seen);
            seen.or(nextA);
        } else {
            nextB.andNot(seen);
            seen.or(nextB);
        }

        // 去除起始节点
        seen.remove(startNodeId);

        return Stream.of(new LongResult((long) seen.getCardinality()));
    }
}   // class
