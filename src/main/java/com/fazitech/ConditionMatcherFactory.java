package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public class ConditionMatcherFactory {
    public static ConditionMatcher getMatcher(JsonNode rule) {
        String condition = rule.path("detection").path("condition").asText();

        switch (condition) {
            case "selection":
                return new SelectionMatcher();
            case "not selection":
                return new NotSelectionMatcher();
            case "all of selection_*":
                return new AllSelectionMatcher();
            case "1 of selection_*":
                return new OneOfSelectionMatcher();
            default:
                throw new IllegalArgumentException("Unsupported condition: " + condition);
        }
    }
}