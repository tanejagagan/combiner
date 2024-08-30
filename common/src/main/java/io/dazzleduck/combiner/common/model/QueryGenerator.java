package io.dazzleduck.combiner.common.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface QueryGenerator {

    String generate(QueryObject queryObject, String format );

    QueryGenerator DUCK_DB = new DuckDBQueryGenerator();

    class DuckDBQueryGenerator implements QueryGenerator {

        public static Set<String> inlineParameter = Set.of("s3_region", "s3_access_key_id",
                "s3_secret_access_key", "s3_session_token", "s3_endpoint", "s3_use_ssl");

        public static Map<String, String> formatFunction = Map.of("parquet", "read_parquet");

        @Override
        public String generate(QueryObject queryObject,
                               String format)  {
            var projections = queryObject.getProjections();
            var sources = queryObject.getSources();
            var predicates = queryObject.getPredicates();
            var aggregations = queryObject.getGroupBys();
            var sortBys = queryObject.getSortBys();
            var parameters = queryObject.getParameters();
            var limit = queryObject.getLimit();

            if(parameters == null) {
                parameters = new HashMap<>();
            }

            String and = " and ";
            String comma = ",";

            StringBuilder buffer = new StringBuilder();
            buffer.append("select ");
            for (String p : projections) {
                buffer.append(p);
                buffer.append(comma);
            }
            buffer.delete(buffer.length() - comma.length(), buffer.length());

            buffer.append(" from ");
            buffer.append(String.format("%s([", formatFunction.get(format)));

            var requiredInlineParameters = new StringBuffer();

            var ampersand = "&";
            parameters.forEach((key, value) -> {
                if (inlineParameter.contains(key)) {
                    requiredInlineParameters.append(key);
                    requiredInlineParameters.append("=");
                    requiredInlineParameters.append(value);
                    requiredInlineParameters.append(ampersand);
                }
            });
            String requiredParameterString;
            if (requiredInlineParameters.length() > 0) {
                requiredInlineParameters.delete(requiredInlineParameters.length() - ampersand.length(),
                        requiredInlineParameters.length());
                requiredParameterString = "?" + requiredInlineParameters.toString();
            } else {
                requiredParameterString = "";
            }

            for (String s : sources) {
                buffer.append("'");
                buffer.append(s);
                buffer.append(requiredParameterString);
                buffer.append("'");
                buffer.append(comma);
            }
            buffer.delete(buffer.length() - comma.length(), buffer.length());
            buffer.append("])");

            if (predicates != null && predicates.size() > 0) {
                buffer.append(" where ");
                for (String p : predicates) {
                    buffer.append(p);
                    buffer.append(and);
                }
                buffer.delete(buffer.length() - and.length(), buffer.length());
            }

            if (aggregations != null && aggregations.size() > 0) {
                buffer.append(" group by ");
                for (String a : aggregations) {
                    buffer.append(a);
                    buffer.append(comma);
                }
                buffer.delete(buffer.length() - comma.length(), buffer.length());
            }

            if (sortBys != null && sortBys.size() > 0) {
                for (String s : sortBys) {
                    buffer.append(s);
                    buffer.append(",");
                }
                buffer.delete(buffer.length() - comma.length(), buffer.length());
            }

            if(limit != null){
                buffer.append("LIMIT ");
                buffer.append(limit);
            }
            return buffer.toString();
        }
    }
}
