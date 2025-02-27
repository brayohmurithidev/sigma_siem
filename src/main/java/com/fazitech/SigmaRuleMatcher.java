package com.fazitech;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class SigmaRuleMatcher {
    public static boolean matchesSigmaRule(JsonNode event, JsonNode rule) {
        System.out.println("Accessing matching function");

        // Check if the logsource matches
        JsonNode ruleLogSource = rule.path("logsource");
        JsonNode detection = rule.path("detection");
        JsonNode selection = detection.path("selection");

        System.out.println("event log: " + detection.path("condition").asText());

        if (detection.path("condition").asText().equals("not selection")) {
            if (!SelectionMatcher.checkSelection(event, selection)) {
                System.out.println("Event passes rule");
            } else {
                System.out.println("Event fails rule");
            }
        } else {
            System.out.println("detection is selection");
        }

        if (!event.path("product").asText().equals(ruleLogSource.path("product").asText())) {
            return false;
        }
        if (!event.path("category").asText().equals(ruleLogSource.path("category").asText())) {
            return false;
        }

        String payload = event.path("payload").asText();

        // Check if all required substrings are in the payload
        ArrayNode containsAll = (ArrayNode) selection.path("Payload contains all");
        for (JsonNode required : containsAll) {
            if (!payload.contains(required.asText())) {
                return false;
            }
        }

        // Check if any of the optional substrings are in the payload
        ArrayNode containsAny = (ArrayNode) selection.path("Payload contains");
        boolean anyMatch = false;
        for (JsonNode optional : containsAny) {
            if (payload.contains(optional.asText())) {
                anyMatch = true;
                break;
            }
        }

        System.out.println("Event: " + event.get("id") + " " + anyMatch);

        return anyMatch;
    }
}
