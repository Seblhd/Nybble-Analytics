package com.nybble.alpha.rule_engine;

import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuleBuilder {

    private static List<String> searchOperators = Arrays.asList("and", "or", "not", "(", ")", "AND", "OR", "NOT", "And", "Or", "Not");
    private static StringBuilder jsonPathRule = new StringBuilder();
    private static StringBuilder finalRuleBuilder = new StringBuilder();
    private static Boolean precededByNot = false;
    private static Boolean toEnclosed = false;
    private static Boolean hasOperators = false;

    public String jsonPathBuilder(Map<String, List<String>> selectionMap, List<String> conditionList) {

        // Reset String builder before using it
        jsonPathRule.setLength(0);

        // Initiate condition ListIterator
        ListIterator<String> conditionIterator = conditionList.listIterator();

        // If condition lit contains any of the search operator, selections need to be enclosed.
        if(conditionList.stream().anyMatch(operators -> searchOperators.contains(operators))) {
            hasOperators = true;
            // Check if enclosment has been correclty done in the rule.
            // If yes, no need to add parenthesis enclosure.
            // If no, set "toEnclosed" to true, to enclose all selections.
            if (conditionList.contains("(") || conditionList.contains(")")) {
                parenthesisEnclosureCheck(conditionList);
            } else {
                toEnclosed = true;
            }
        }

        // Before any other element, append the first parenthesis if toEnclosed is true.
        if (toEnclosed) {
            jsonPathRule.append("(");
        }

        while (conditionIterator.hasNext()) {
            String condition = conditionIterator.next();

            if(searchOperators.contains(condition)) {
                if(condition.equals("and")) {
                    jsonPathRule.append(" && ");
                } else if(condition.equals("or")) {
                    jsonPathRule.append(" || ");
                } else if (condition.equals("not")){
                    // Check if next value is operator "1 of"
                    if(conditionIterator.next().equals("1 of")) {
                        precededByNot = true;
                        // If equal is true, Go back to previous value after check of next value
                        conditionIterator.previous();
                    } else if (conditionIterator.previous().equals("all of")) {
                        // Check previous value because of the next done in previous If statement
                        precededByNot = true;
                    } else {
                        // No need to move, Next on 1st If statement and previous on 2nd If statement
                        jsonPathRule.append("!");
                    }
                } else {
                    jsonPathRule.append(condition);
                }
            } else if(condition.equals("1 of")) {
                // Get expression next to "1 of" operator
                String nextCondition = conditionIterator.next();

                if(nextCondition.endsWith("*")) {

                    // Init counter for OR condition
                    int matchCount = 0;
                    int count = matchCount(nextCondition, selectionMap);
                    boolean firstEnclose = false;

                    for(String key: selectionMap.keySet()) {

                        Matcher fieldParser = Pattern.compile(nextCondition.substring(0, nextCondition.length() -1)+".*").matcher(key);

                        while(fieldParser.find()) {
                            if (precededByNot){
                                if (!firstEnclose) {
                                    jsonPathRule.append("!").append("(");
                                    firstEnclose = true;
                                }
                                listConverter(selectionMap, key);
                            } else if (!precededByNot){
                                if (!firstEnclose) {
                                    jsonPathRule.append("(");
                                    firstEnclose = true;
                                }
                                listConverter(selectionMap, key);
                            }
                            if(matchCount < count - 1) {
                                jsonPathRule.append(" || ");
                            }
                            matchCount++;
                        }
                    }
                    jsonPathRule.append(")");
                } else {
                    if (precededByNot){
                        for(String selectValue : selectionMap.get(nextCondition)) {
                            jsonPathRule.append("!").append(selectValue);
                            for(int x = 1;x  < selectionMap.get(nextCondition).size(); x++) {
                                jsonPathRule.append(" && ");
                            }
                        }
                    } else if (!precededByNot){
                        for(String selectValue : selectionMap.get(nextCondition)) {
                            jsonPathRule.append(selectValue);
                            for(int x = 1;x  < selectionMap.get(nextCondition).size(); x++) {
                                jsonPathRule.append(" && ");
                            }
                        }
                    }
                }
                // Switch preceded to false for next "not"
                precededByNot = false;
            } else if (condition.equals("all of")) {
                // Get expression next to "all of" operator
                String nextCondition = conditionIterator.next();

                if(nextCondition.endsWith("*")) {

                    // Init counter for AND condition
                    int matchCount = 0;
                    int count = matchCount(nextCondition, selectionMap);
                    boolean firstEnclose = false;

                    for(String key: selectionMap.keySet()) {

                        Matcher fieldParser = Pattern.compile(nextCondition.substring(0, nextCondition.length() -1)+".*").matcher(key);

                        while(fieldParser.find()) {
                            if (precededByNot){
                                if (!firstEnclose) {
                                    jsonPathRule.append("!").append("(");
                                    firstEnclose = true;
                                }
                                listConverter(selectionMap, key);
                            } else if (!precededByNot){
                                if (!firstEnclose) {
                                    jsonPathRule.append("(");
                                    firstEnclose = true;
                                }
                                listConverter(selectionMap, key);
                            }
                            if(matchCount < count - 1) {
                                jsonPathRule.append(" || ");
                            }
                            matchCount++;
                        }
                    }
                    jsonPathRule.append(")");
                } else {
                    if (precededByNot){
                        for(String selectValue : selectionMap.get(nextCondition)) {
                            jsonPathRule.append("!").append(selectValue);
                            for(int x = 1;x  < selectionMap.get(nextCondition).size(); x++) {
                                jsonPathRule.append(" && ");
                            }
                        }
                    } else if (!precededByNot){
                        for(String selectValue : selectionMap.get(nextCondition)) {
                            jsonPathRule.append(selectValue);
                            for(int x = 1;x  < selectionMap.get(nextCondition).size(); x++) {
                                jsonPathRule.append(" && ");
                            }
                        }
                    }
                }
                // Switch preceded to false for next "not"
                precededByNot = false;
            } else if(condition.equals("1 of them")){
                int keyCount = 0;

                for(String key: selectionMap.keySet()) {

                    listConverter(selectionMap, key);

                    if(keyCount < selectionMap.keySet().size() -1) {
                        jsonPathRule.append(" || ");
                    }
                    keyCount++;
                }
            } else if(condition.equals("all of them")){
                int keyCount = 0;

                for(String key: selectionMap.keySet()) {

                    listConverter(selectionMap, key);

                    if(keyCount < selectionMap.keySet().size() -1) {
                        jsonPathRule.append(" && ");
                    }
                    keyCount++;
                }
            } else if(!searchOperators.contains(condition)) {

                listConverter(selectionMap, condition);
            }
        }

        // After all other element, append the last parenthesis if toEnclosed is true.
        // Then set toEnclosed to false.
        if (toEnclosed) {
            jsonPathRule.append(")");
            toEnclosed = false;
        }

        // Add last operators with or without parenthesis.
        String finalRule = finalRuleOperator(jsonPathRule);

        return finalRule;
    }

    private Integer matchCount(String nextCondition, Map<String, List<String>> selectionMap){

        int matchCount = 0;
        for(String key: selectionMap.keySet()) {
            Matcher fieldParser = Pattern.compile(nextCondition.substring(0, nextCondition.length() -1)+".*").matcher(key);
            while(fieldParser.find()){
                matchCount++;
            }
        }
        return matchCount;
    }

    private void listConverter (Map<String, List<String>> selectionMap ,String key) {
        AtomicInteger x = new AtomicInteger();
        boolean multiValueList = false;

        if (selectionMap.get(key).size() > 1) {
            multiValueList = true;
            jsonPathRule.append("(");
        }

        // For each selection fields, get the value and append to jsonPath rule string builder.
        // Each value in selection field list is linked by AND Operator.
        // If there is more than one value in selection field list for current key, then values need to be enclosed with parenthesis.
        for(String selectValue : selectionMap.get(key)) {
            jsonPathRule.append(selectValue);
            if(x.get() + 1 < selectionMap.get(key).size()) {
                x.getAndIncrement();
                jsonPathRule.append(" && ");
            }
        }

        if (multiValueList) {
            jsonPathRule.append(")");
        }
    }

    private String finalRuleOperator(StringBuilder jsonPathRule) {
        // Reset String builder before using it
        finalRuleBuilder.setLength(0);

        if (hasOperators) {
            finalRuleBuilder.append("$[?").append(jsonPathRule).append("]");
            // Reset hasOperators at end of final building
            hasOperators = false;
        } else {
            finalRuleBuilder.append("$[?(").append(jsonPathRule).append(")]");
            // Reset hasOperators at end of final building
            hasOperators = false;
        }

        return finalRuleBuilder.toString();
    }

    private void parenthesisEnclosureCheck(List<String> conditionList) {

        boolean opBetweenEnclosure = false;
        int listSize = conditionList.size();
        int parenthesisPair = 0;

        for (String condition : conditionList) {
            if (condition.equals("(")) {
                parenthesisPair += 1;
            } else if (condition.equals(")")) {
                parenthesisPair -= 1;
            }
            // If there is some search operators between parenthesis, then switch Operators Between enclosure to true.
            // In this case, the whole rule will need to be enclosed.
            if (parenthesisPair == 0 && (condition.toLowerCase().equals("and")
                    || condition.toLowerCase().equals("or")
                    || condition.toLowerCase().equals("not"))) {
                opBetweenEnclosure = true;
            }
        }

        if (conditionList.get(0).equals("(") && conditionList.get(listSize-1).equals(")") && parenthesisPair==0 && !opBetweenEnclosure) {
            toEnclosed = false;
        } else {
            toEnclosed =true;
        }
    }
}