package com.hortonworks.iotas.layout.design.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by pshah on 11/19/15.
 */
public class RulesProcessorJsonBuilder implements RulesProcessorBuilder {
    protected static final Logger log = LoggerFactory.getLogger(RulesProcessorJsonBuilder.class);
    private final String json;
    RulesProcessorJsonBuilder (String json) {
        this.json = json;
    }

    @Override
    public RulesProcessor getRulesProcessor () {
        ObjectMapper mapper = new ObjectMapper();
        RulesProcessor rulesProcessor = null;
        try {
            rulesProcessor = mapper.readValue(json, RulesProcessor.class);
        } catch (IOException e) {
            log.error("Error creating RulesProcessor from json string. Check " +
                    "the json");
            e.printStackTrace();
        }
        return rulesProcessor;
    }
}
