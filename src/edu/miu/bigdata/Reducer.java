package edu.miu.bigdata;

import java.util.ArrayList;
import java.util.List;

public class Reducer {
    private final List<Pair> groupByPair = new ArrayList<>();

    public List<Pair> getGroupBySum() {
        return groupBySum;
    }

    private final List<Pair> groupBySum = new ArrayList<>();

    public void reduce(String key, List<Integer> listItems) {
        int sum = 1;
        for (int i = 0; i < listItems.size(); i++) {
            sum++;
        }
        groupBySum.add(new Pair(key, sum));
    }

    public void printReducerOutput() {
        groupBySum.stream().forEach(x -> System.out.println(x.toString()));
        System.out.println(groupBySum.size());
    }
}
