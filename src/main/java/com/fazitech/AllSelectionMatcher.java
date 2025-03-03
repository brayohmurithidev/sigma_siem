package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public class AllSelectionMatcher implements ConditionMatcher {
    @Override
    public boolean matches(JsonNode event, JsonNode detection) {
        for (java.util.Iterator<String> it = detection.fieldNames(); it.hasNext(); ) {
            String fieldName = it.next();
            if (fieldName.startsWith("selection_")) {
                JsonNode selection = detection.path(fieldName);
                if (!new SelectionMatcher().checkSelection(event, selection)) {
                    return false;
                }
            }
        }
        return true;
    }
}