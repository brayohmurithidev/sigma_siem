package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public interface ConditionMatcher {
    boolean matches(JsonNode event, JsonNode detection);
}