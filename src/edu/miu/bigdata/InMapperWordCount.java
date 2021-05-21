package edu.miu.bigdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InMapperWordCount {
    private final int splitNo;
    private final int reducerNo;

    public InMapperWordCount(int splitNo, int reducerNo) {
        this.splitNo = splitNo;
        this.reducerNo = reducerNo;
    }

    public HashMap<Integer, List<Pair>> GroupPairToReduce(Mapper mapper, int reducerNo) {
        HashMap<String, Integer> mapperOutput = mapper.getAssociateArr();
        HashMap<Integer, List<Pair>> hmap = new HashMap<>();

        mapperOutput.forEach(((key, value) -> {
            int index = WordCount.getPartition(key, reducerNo);
            for (int j = 0; j < reducerNo; j++) {
                if (index == j) {
                    if (hmap.containsKey(index)) {
                        hmap.get(index).add(new Pair(key, value));
                        hmap.put(index, hmap.get(index));
                    } else {
                        List<Pair> lp = new ArrayList<>();
                        lp.add(new Pair(key, value));
                        hmap.put(index, lp);
                    }
                }
            }
        }));
        return hmap;
    }

    public static void main(String[] args) {
        int SPLIT_NO = 3;
        int REDUCER_NO = 4;

        InMapperWordCount inMapperWordCount = new InMapperWordCount(SPLIT_NO, REDUCER_NO);
        String split0 = "\"cat bat\" mat-pat mum.edu sat.\n" +
                "fat 'rat eat cat' mum_cs mat";
        String split1 = "bat-hat mat pat \"oat\n" +
                "hat rat mum_cs eat oat-pat";
        String split2 = "zat lat-cat pat jat.\n" +
                "hat rat. kat sat wat";

        String[] splits = new String[]{split0, split1, split2};

        Mapper[] mapperList = new Mapper[SPLIT_NO];

        System.out.println(" Number of Input-Splits: " + SPLIT_NO);
        System.out.println("Number of Reducers " + REDUCER_NO);

        for (int i = 0; i < SPLIT_NO; i++) {
            Mapper mapper = new Mapper(splits[i]);
            mapper.doMap();
            mapperList[i] = mapper;

            System.out.println("Mapper " + i + " output");
            mapper.getAssociateArr().forEach((key, value) -> System.out.println("< " + key + ",  " + value + " >"));
        }

        HashMap<Integer, List<Pair>> reducerInputsByNumber = new HashMap<>();
        for (int i = 0; i < mapperList.length; i++) {
            Mapper mapper = mapperList[i];
            HashMap<Integer, List<Pair>> hmap = inMapperWordCount.GroupPairToReduce(mapper, REDUCER_NO);
            int finalI = i;
            hmap.forEach((rn, listPairs) -> {
                if (reducerInputsByNumber.containsKey(rn)) {
                    List<Pair> listP = reducerInputsByNumber.get(rn);
                    listP.addAll(listPairs);
                    reducerInputsByNumber.put(rn, listP);
                } else {
                    reducerInputsByNumber.put(rn, listPairs);
                }

                System.out.println("Pairs send from Mapper " + finalI + " Reducer " + rn);
                for (Pair p : listPairs) {
                    System.out.println(p.toString());
                }
            });
        }


        reducerInputsByNumber.forEach((rn, listPairs) -> {
            HashMap<String, List<Integer>> reducerDataInputByKey = new HashMap<>();
            for (Pair p : listPairs) {
                if (reducerDataInputByKey.containsKey(p.key)) {
                    List<Integer> liIn = reducerDataInputByKey.get(p.key);
                    liIn.add((Integer) p.value);
                    reducerDataInputByKey.put((String) p.key, liIn);
                } else {
                    List<Integer> dt = new ArrayList<>();
                    dt.add((Integer) p.value);
                    reducerDataInputByKey.put((String) p.key, dt);
                }
            }
            System.out.println("Reducer " + rn + " input");
            reducerDataInputByKey.forEach((key, value) -> System.out.println("< " + key + ",  " + value + " >"));
        });

        reducerInputsByNumber.forEach((rn, listPairs) -> {
            HashMap<String, List<Integer>> reducerDataInput = new HashMap<>();
            for (Pair p : listPairs) {
                if (reducerDataInput.containsKey(p.key)) {
                    List<Integer> liIn = reducerDataInput.get(p.key);
                    liIn.add((Integer) p.value);
                    reducerDataInput.put((String) p.key, liIn);
                } else {
                    List<Integer> dt = new ArrayList<>();
                    dt.add((Integer) p.value);
                    reducerDataInput.put((String) p.key, dt);
                }
            }

            System.out.println("Reducer " + rn + " output");
            reducerDataInput.forEach((key, value) -> {
                Reducer rd = new Reducer();
                rd.reduce(key, value);
                List<Pair> afterReduce = rd.getGroupBySum();
                for (Pair p : afterReduce) {
                    System.out.println(p.toString());
                }
            });
        });
    }
}
