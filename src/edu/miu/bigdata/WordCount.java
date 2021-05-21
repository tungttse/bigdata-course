package edu.miu.bigdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class WordCount {
    public static int getPartition(String key, int reducerNo) {
        return Math.abs(key.hashCode()) % reducerNo;
    }

    public HashMap<Integer, List<Pair>> GroupPairToReduce(Mapper mapper, int reducerNo) {
        List<Pair> mapperOutput = mapper.getMapperOutput();
        HashMap<Integer, List<Pair>> hmap = new HashMap<>();
        for (Pair item : mapperOutput) {
            int index = WordCount.getPartition((String) item.key, reducerNo);
            for (int j = 0; j < reducerNo; j++) {
                if (index == j) {
                    if (hmap.containsKey(index)) {
                        hmap.get(index).add(item);
                    } else {
                        List<Pair> lp = new ArrayList<>();
                        lp.add(item);
                        hmap.put(index, lp);
                    }
                }
            }
        }

        return hmap;
    }

    public static void main(String[] args) {
        int mapperNo = 4;
        int reducerNo = 3;
        WordCount wc = new WordCount();
        String filePath = "./src/edu/miu/bigdata/assets/input.txt";
        String data = Utils.ReadFile(filePath);
        Mapper[] mapperList = new Mapper[mapperNo];

        System.out.println(" Number of Input-Splits: " + mapperNo);
        System.out.println("Number of Reducers " + reducerNo);

        String[] words = data.split("\\s+");
        int splitSize = (int) Math.ceil((double) words.length / (double) mapperNo);

        for (int i = 0; i < mapperNo; i++) {
            int fromIndex = i * splitSize;
            String[] splitItem = Arrays.copyOfRange(words, fromIndex, fromIndex + splitSize);
            String splitData = Arrays.stream(splitItem).filter(x -> x != null).reduce("", (subtotal, element) -> subtotal + element + " ");
            Mapper mapper = new Mapper(splitData);
            mapper.doMap();

            mapperList[i] = mapper;

            System.out.println("Mapper " + i + " input");
            System.out.println(splitData);

            System.out.println("Mapper " + i + " output");
            mapper.printMapperOutput();
        }

        for (int i = 0; i < mapperList.length; i++) {
            Mapper mapper = mapperList[i];
            HashMap<Integer, List<Pair>> hmap = wc.GroupPairToReduce(mapper, reducerNo);
            int finalI = i;
            hmap.forEach((rn, listPairs) -> {
                System.out.println("Pairs send from Mapper " + finalI + " Reducer " + rn);
                for (Pair p : listPairs) {
                    System.out.println(p.toString());
                }
            });
        }

        for (int i = 0; i < mapperList.length; i++) {
            Mapper mapper = mapperList[i];
            HashMap<Integer, List<Pair>> hmap = wc.GroupPairToReduce(mapper, reducerNo);
            hmap.forEach((rn, listPairs) -> {
                System.out.println("Reducer " + rn + " input");
                mapper.generateReducerInput(listPairs);
                mapper.printReducerInput();
            });
        }

        for (int i = 0; i < mapperList.length; i++) {
            Mapper mapper = mapperList[i];
            HashMap<Integer, List<Pair>> hmap = wc.GroupPairToReduce(mapper, reducerNo);
            hmap.forEach((rn, listPairs) -> {
                System.out.println("Reducer " + rn + " output");
                mapper.generateReducerInput(listPairs);
                List<Pair> rs = mapper.doReducer(mapper.getReducerInput());
                mapper.printPairs(rs);
            });
        }
    }

}
