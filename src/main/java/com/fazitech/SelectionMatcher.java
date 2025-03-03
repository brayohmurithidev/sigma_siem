package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public class SelectionMatcher implements ConditionMatcher {
    @Override
    public boolean matches(JsonNode event, JsonNode detection) {
        JsonNode selection = detection.path("selection");
        return checkSelection(event, selection);
    }

    protected boolean checkSelection(JsonNode event, JsonNode selection) {
        for (java.util.Iterator<String> it = selection.fieldNames(); it.hasNext(); ) {
            String fieldWithModifier = it.next();
            String fieldName = fieldWithModifier.split("\\|")[0];
            String modifier = fieldWithModifier.contains("|") ? fieldWithModifier.split("\\|")[1] : null;
            JsonNode fieldValue = selection.path(fieldWithModifier);

            if (!evaluateField(event, fieldName, modifier, fieldValue)) {
                return false;
            }
        }
        return true;
    }

    private boolean evaluateField(JsonNode event, String fieldName, String modifier, JsonNode fieldValue) {
        if (!event.has(fieldName)) {
            return false; // Field does not exist in the event
        }

        String eventValue = event.path(fieldName).asText();

        if (modifier != null) {
            switch (modifier) {
                case "contains":
                    return fieldValue.asText().contains(eventValue);
                case "cidr":
                    return isIpInCidrRanges(eventValue, fieldValue);
                default:
                    throw new IllegalArgumentException("Unsupported modifier: " + modifier);
            }
        }

        if (fieldValue.isTextual()) {
            return eventValue.equals(fieldValue.asText());
        } else if (fieldValue.isArray()) {
            for (JsonNode value : fieldValue) {
                if (eventValue.equals(value.asText())) {
                    return true;
                }
            }
            return false;
        }

        return false;
    }

    private boolean isIpInCidrRanges(String ip, JsonNode cidrRanges) {
        for (JsonNode cidr : cidrRanges) {
            if (isIpInCidr(ip, cidr.asText())) {
                return true;
            }
        }
        return false;
    }

    private boolean isIpInCidr(String ip, String cidr) {
        // Implement CIDR range checking logic here (e.g., using a library like Apache Commons Net or custom logic)
        // For simplicity, this is a placeholder implementation.
        return ip.startsWith(cidr.split("/")[0]);
    }
}