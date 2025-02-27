package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;

public class SelectionMatcher {
    public static boolean checkSelection(JsonNode event, JsonNode selection) {
        System.out.println("On selection function: " + selection);
        // Iterate over the fields in the selection
        for (java.util.Iterator<String> it = selection.fieldNames(); it.hasNext(); ) {
            String fieldWithModifier = it.next();
            String fieldName = fieldWithModifier.split("\\|")[0]; // Split on pipe and take the first part
            System.out.println("Field: " + fieldName);
            JsonNode fieldValue = selection.path(fieldWithModifier);
            System.out.println("Value: " + fieldValue);

            // Handle single value (exact match)
            if (fieldValue.isTextual()) {
                if (!event.path(fieldName).asText().equals(fieldValue.asText())) {
                    return false;
                }
            }
            // Handle list of values (OR operation)
            else if (fieldValue.isArray()) {
                boolean anyMatch = false;
                for (JsonNode value : fieldValue) {
                    System.out.println("Value single: " + value + event.path(fieldName));
                    if (event.path(fieldName).asText().equals(value.asText())) {
                        System.out.println("Matched value: " + value);
                        anyMatch = true;
                        break;
                    }
                }
                if (!anyMatch) {
                    return anyMatch;
                }
            }
        }

        // If all fields in the selection match, return true
        return true;
    }
}
