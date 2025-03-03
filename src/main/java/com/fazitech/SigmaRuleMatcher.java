package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public class SigmaRuleMatcher {
    public static boolean matchesSigmaRule(JsonNode event, JsonNode rule) {
        System.out.println("Accessing matching function");

        // Get the detection section from the rule
        JsonNode detection = rule.path("detection");

        // Get the appropriate matcher based on the condition
        ConditionMatcher matcher = ConditionMatcherFactory.getMatcher(rule);
        return matcher.matches(event, detection);
    }
}