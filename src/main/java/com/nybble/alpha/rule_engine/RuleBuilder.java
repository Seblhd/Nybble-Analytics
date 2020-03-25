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
    private static Boolean hasSearchOperator = false;

    public String jsonPathBuilder(Map<String, List<String>> selectionMap, List<String> conditionList) {

        // Reset String builder before using it
        jsonPathRule.setLength(0);

        // Initiate condition ListIterator
        ListIterator<String> conditionIterator = conditionList.listIterator();

        if(conditionList.stream().anyMatch(operators -> searchOperators.contains(operators))) {
            // TODO
            // Remove ( and ) from operators list and check in another part of loop.
            // Append ( at the beginning of jsonPathRule if hasSearchOperator is true
            // At the end of the loop append ) of jsonPathRule if hasSearchOperator is true and switch back to false after append.
            System.out.println("Condition list is containing operators from search operators list");
            System.out.println("Condition list value : " + conditionList);
            hasSearchOperator = true;
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

                    for(String key: selectionMap.keySet()) {

                        Matcher fieldParser = Pattern.compile(nextCondition.substring(0, nextCondition.length() -1)+".*").matcher(key);

                        while(fieldParser.find()){
                            if (precededByNot){
                                jsonPathRule.append("!").append("(");
                                listConverter(selectionMap, key);
                                jsonPathRule.append(")");
                            } else if (!precededByNot){
                                jsonPathRule.append("(");
                                listConverter(selectionMap, key);
                                jsonPathRule.append(")");
                            }
                            if(matchCount < count - 1) {
                                jsonPathRule.append(" || ");
                            }
                            matchCount++;
                        }
                    }
                } else {
                    if (precededByNot){
                        jsonPathRule.append("!").append(nextCondition);
                    } else if (!precededByNot){
                        jsonPathRule.append(nextCondition);
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

                    for(String key: selectionMap.keySet()) {

                        Matcher fieldParser = Pattern.compile(nextCondition.substring(0, nextCondition.length() -1)+".*").matcher(key);

                        while(fieldParser.find()){
                            if (precededByNot){
                                jsonPathRule.append("!").append("(");
                                listConverter(selectionMap, key);
                                jsonPathRule.append(")");
                            } else if (!precededByNot){
                                jsonPathRule.append("(");
                                listConverter(selectionMap, key);
                                jsonPathRule.append(")");
                            }
                            if(matchCount < count - 1) {
                                jsonPathRule.append(" && ");
                            }
                            matchCount++;
                        }
                    }
                } else {
                    if (precededByNot){
                        jsonPathRule.append("!").append(nextCondition);
                    } else if (!precededByNot){
                        jsonPathRule.append(nextCondition);
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

    private void listConverter (Map<String, List<String>> selectionMap ,String condition) {
        AtomicInteger x = new AtomicInteger();

        for(String selectValue : selectionMap.get(condition)) {
            jsonPathRule.append(selectValue);
            if(x.get() + 1 < selectionMap.get(condition).size()) {
                x.getAndIncrement();
                jsonPathRule.append(" && ");
            }
        }
    }

    private String finalRuleOperator(StringBuilder jsonPathRule) {
        // Reset String builder before using it
        finalRuleBuilder.setLength(0);

        if (jsonPathRule.toString().startsWith("(") && jsonPathRule.toString().endsWith(")")) {
            finalRuleBuilder.append("$[?").append(jsonPathRule).append("]");
        } else {
            finalRuleBuilder.append("$[?(").append(jsonPathRule).append(")]");
        }

        return finalRuleBuilder.toString();
    }
}