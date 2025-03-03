package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public class NotSelectionMatcher implements ConditionMatcher {
    @Override
    public boolean matches(JsonNode event, JsonNode detection) {
        JsonNode selection = detection.path("selection");
        return !new SelectionMatcher().checkSelection(event, selection);
    }
}