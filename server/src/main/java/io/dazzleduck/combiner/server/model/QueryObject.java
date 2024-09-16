package io.dazzleduck.combiner.server.model;

import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.Serdeable;

import java.util.List;
import java.util.Map;

/**
 * This object is created only because getting io.dazzleduck.combiner.common.model.QueryObject to work with CombinerController is tricky and taking lot of time
 * In the future this can be removed once right configuration is sorted out.
 */
@Introspected
@Serdeable
public class QueryObject {

    private List<String> projections;
    private List<String> sources;
    private List<String> predicates;
    private List<String> sortBys;
    private List<String> groupBys;
    private Map<String, String> parameters;
    private Integer limit;

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public List<String> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<String> predicates) {
        this.predicates = predicates;
    }

    public List<String> getSortBys() {
        return sortBys;
    }

    public void setSortBys(List<String> sortBys) {
        this.sortBys = sortBys;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public void setGroupBys(List<String> groupBys) {
        this.groupBys = groupBys;
    }

    public List<String> getSources() {
        return sources;
    }

    public void setSources(List<String> sources) {
        this.sources = sources;
    }

    public List<String> getProjections() {
        return projections;
    }

    public void setProjections(List<String> projections) {
        this.projections = projections;
    }

    public io.dazzleduck.combiner.common.model.QueryObject toCommonQueryObject(){
        return io.dazzleduck.combiner.common.model.QueryObject.builder()
                .projections(projections)
                .predicates(predicates)
                .sources(sources)
                .groupBys(groupBys).sortBys(sortBys).parameters(parameters).limit(limit).build();
    }
}



