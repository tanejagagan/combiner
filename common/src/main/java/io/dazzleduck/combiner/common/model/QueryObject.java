package io.dazzleduck.combiner.common.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonDeserialize(builder = QueryObject.Builder.class)
public class QueryObject {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryObject that = (QueryObject) o;
        return Objects.equals(projections, that.projections) &&
                Objects.equals(predicates, that.predicates) &&
                Objects.equals(groupBys, that.groupBys) &&
                Objects.equals(sources, that.sources) &&
                Objects.equals(sortBys, that.sortBys) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projections, predicates, groupBys, sources, sortBys, limit);
    }

    private final List<String> projections;
    private final List<String> predicates;
    private final List<String> groupBys;
    private final List<String> sources;
    private final List<String> sortBys;
    private final Map<String, String > parameters;
    private final Integer limit;

    public List<String> getProjections() {
        return projections;
    }

    public List<String> getPredicates() {
        return predicates;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public List<String> getSources() {
        return sources;
    }

    public List<String> getSortBys() {
        return sortBys;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public Integer getLimit() {return limit; }

    public QueryObject(List<String> projections,
                       List<String> predicates,
                       List<String> groupBys,
                       List<String> sources,
                       List<String> sortBys,
                       Map<String, String> parameters,
                       Integer limit) {
        this.projections = projections;
        this.predicates = predicates;
        this.groupBys = groupBys;
        this.sources = sources;
        this.sortBys = sortBys;
        this.parameters = parameters;
        this.limit = limit;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(QueryObject object){
        return new Builder().projections(object.projections)
                .predicates(object.predicates)
                .sources(object.sources)
                .groupBys(object.groupBys)
                .sortBys(object.sortBys)
                .parameters(object.getParameters())
                .limit(object.limit);
    }


    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private List<String> projections;
        private List<String> predicates;
        private List<String> groupBys;
        private List<String> sources;
        private List<String> sortBys;
        private Map<String, String> parameters;
        private Integer limit;

        public Builder(){

        }

        public QueryObject build() {
            return new QueryObject(projections, predicates, groupBys, sources, sortBys, parameters, limit);
        }

        public Builder projections(List<String> projections) {
            this.projections = projections;
            return this;
        }

        public Builder predicates(List<String> predicates) {
            this.predicates = predicates;
            return this;
        }

        public Builder groupBys(List<String> groupBys) {
            this.groupBys = groupBys;
            return this;
        }

        public Builder sources(List<String> sources) {
            this.sources = sources;
            return this;
        }

        public Builder sortBys(List<String> sortBy) {
            this.sortBys = sortBy;
            return this;
        }

        public Builder parameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder limit(Integer limit){
            this.limit = limit;
            return this;
        }
    }
}