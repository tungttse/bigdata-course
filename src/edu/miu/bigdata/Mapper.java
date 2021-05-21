package edu.miu.bigdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Mapper {
    private String filteredData = "";
    private final String data;

    public List<Pair> getMapperOutput() {
        return mapperOutput;
    }

    private final List<Pair> mapperOutput = new ArrayList<>();
    private final List<GroupByPair> reducerInput = new ArrayList<>();

    public HashMap<String, Integer> getAssociateArr() {
        return associateArr;
    }

    private final HashMap<String, Integer> associateArr;

    Mapper(String data) {
        this.data = data;
        associateArr = new HashMap();
    }

    public void doMap() {
        this._generateMapperOutput(data);
        this.generateReducerInput(mapperOutput);
        this.doReducer(reducerInput);

        mapperOutput.forEach(item -> {
            if (associateArr.containsKey(item.key)) {
                associateArr.put((String) item.key, associateArr.get(item.key) + 1);
            } else {
                associateArr.put((String) item.key, 1);
            }
        });
    }

    public List<GroupByPair> getReducerInput() {
        return reducerInput;
    }

    private String _filterWord(String word) {
        String pattern = ".*[\\d_.].*";
        if (word.matches(pattern)) return "";
        return word.replaceAll("\\W", "");
    }

    public void generateReducerInput(List<Pair> mapperOutput) {
        for (int i = 0; i < mapperOutput.size(); i++) {
            if (mapperOutput.get(i).value == "") continue;

            List<Integer> valueCount = new ArrayList<>();
            valueCount.add(1);

            for (int j = i + 1; j < mapperOutput.size(); j++) {
                if (mapperOutput.get(i).key.equals(mapperOutput.get(j).key)) {
                    valueCount.add(1);
                    mapperOutput.set(j, new Pair(mapperOutput.get(j).key, ""));
                }
            }
            reducerInput.add(new GroupByPair(mapperOutput.get(i).key, valueCount));
        }
    }

    public void generateReducerInputNew(HashMap<String, Integer> associateArr) {
        for (int i = 0; i < mapperOutput.size(); i++) {
            if (mapperOutput.get(i).value == "") continue;

            List<Integer> valueCount = new ArrayList<>();
            valueCount.add(1);

            for (int j = i + 1; j < mapperOutput.size(); j++) {
                if (mapperOutput.get(i).key.equals(mapperOutput.get(j).key)) {
                    valueCount.add(1);
                    mapperOutput.set(j, new Pair(mapperOutput.get(j).key, ""));
                }
            }
            reducerInput.add(new GroupByPair(mapperOutput.get(i).key, valueCount));
        }
    }

    private void _filterAndSortData(String data) {
        String[] words = data.split("\\s+");
        List<String> listWords = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            String wordHandle = words[i];
            String[] wordsCheck = wordHandle.split("-");
            if (wordsCheck.length > 1) {
                for (String w : wordsCheck) {
                    listWords.add(w.toLowerCase());
                }
                continue;
            }
            listWords.add(wordHandle.toLowerCase());
        }

        // handle word has end char is . or , and ""
        List<String> listWordsFinal = new ArrayList<>();
        listWords.forEach(w -> {
            String lastChar = w.substring(w.length() - 1);
            if (lastChar.equals(".") || lastChar.equals(",")) {
                w = w.substring(0, w.length() - 1);
            }
            w = w.replace("\"", "");
            if (!this._filterWord(w).equals("")) {
                listWordsFinal.add(w);
            }
        });

        //sorted
        listWordsFinal.stream().sorted().forEach(w -> {
            filteredData += this._filterWord(w) + " ";
        });
    }

    private void _generateMapperOutput(String data) {
        this._filterAndSortData(data);
        String[] words = filteredData.split("\\s+");
        for (String w : words) {
            mapperOutput.add(new Pair(w, 1));
        }
    }

    public List<Pair> doReducer(List<GroupByPair> reducerInput) {
        Reducer r = new Reducer();
        reducerInput.forEach(item -> {
            r.reduce((String) item.key, (List<Integer>) item.value);
        });
        return r.getGroupBySum();
    }

    public void printMapperOutput() {
        mapperOutput.stream().forEach(x -> System.out.println(x.toString()));
    }

    public void printReducerInput() {
        reducerInput.stream().forEach(x -> System.out.println(x.toString()));
    }

    public void printPairs(List<Pair> p) {
        p.stream().forEach(x -> System.out.println(x.toString()));
    }
}
