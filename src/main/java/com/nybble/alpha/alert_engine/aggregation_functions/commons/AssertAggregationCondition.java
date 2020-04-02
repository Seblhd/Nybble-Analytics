package com.nybble.alpha.alert_engine.aggregation_functions.commons;

public class AssertAggregationCondition {

    public Boolean conditionResult (String aggregatorOperator, Long currentMapCount , Long aggregatorValue) {

        // Following each aggregation operator check aggregation value.
        switch (aggregatorOperator) {
            case ">":
                if (currentMapCount > aggregatorValue) {
                    return true;
                }
                break;
            case ">=":
                if (currentMapCount >= aggregatorValue) {
                    return true;
                }
                break;
            case "<":
                if (currentMapCount < aggregatorValue) {
                    return true;
                }
                break;
            case "<=":
                if (currentMapCount <= aggregatorValue) {
                    return true;
                }
                break;
            case "=":
                if (currentMapCount.equals(aggregatorValue)) {
                    return true;
                }
                break;
        }

        return false;
    }
}
