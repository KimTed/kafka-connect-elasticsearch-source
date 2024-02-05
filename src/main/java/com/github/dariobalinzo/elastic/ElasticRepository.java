/**
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.elastic;

import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.PageResult;
import org.apache.commons.codec.binary.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.dariobalinzo.elastic.ElasticJsonNaming.removeKeywordSuffix;
import static org.elasticsearch.index.query.QueryBuilders.*;

public final class ElasticRepository {
    private final static Logger logger = LoggerFactory.getLogger(ElasticRepository.class);

    private final ElasticConnection elasticConnection;

    private final String cursorSearchField;
    private final String secondaryCursorSearchField;
    private final CursorField cursorField;
    private final CursorField secondaryCursorField;

    private int pageSize = 5000;
    private final int MAX_SEARCH_LIMIT = 10000;

    public ElasticRepository(ElasticConnection elasticConnection) {
        this(elasticConnection, "_id");
    }

    public ElasticRepository(ElasticConnection elasticConnection, String cursorField) {
        this(elasticConnection, cursorField, null);
    }

    public ElasticRepository(ElasticConnection elasticConnection, String cursorSearchField, String secondaryCursorSearchField) {
        this.elasticConnection = elasticConnection;
        this.cursorSearchField = cursorSearchField;
        this.cursorField = new CursorField(cursorSearchField);
        this.secondaryCursorSearchField = secondaryCursorSearchField;
        this.secondaryCursorField = secondaryCursorSearchField == null ? null : new CursorField(secondaryCursorSearchField);
    }

    public PageResult searchAfter(String index, Cursor cursor) throws IOException, InterruptedException {
        System.out.println("@@@@@@@@@@@@@@@ searchAfter() call @@@@@@@@@@@@@@");
        QueryBuilder queryBuilder = cursor.getPrimaryCursor() == null ?
                matchAllQuery() :
                buildGreaterThen(cursorSearchField, cursor.getPrimaryCursor());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .size(pageSize)
//                .sort(cursorSearchField, SortOrder.DESC)
                .sort("_id", SortOrder.DESC)
                .trackTotalHits(true);

        int totalCount = 0;
        List<Map<String, Object>> documents = new ArrayList<>();
        List<String> pivotIdList = new ArrayList<>();
        List<String> dupIdList = new ArrayList<>();
int tmp = 0;
        do {
            tmp++;
            SearchRequest searchRequest = new SearchRequest(index)
                .source(searchSourceBuilder);

            SearchResponse response = executeSearch(searchRequest);
            System.out.println("Hits is null ? " + response.getHits());
            System.out.println("getTotalHits is null ? " + response.getHits().getTotalHits());
            totalCount = (int) response.getHits().getTotalHits().value;

            System.out.println("totalCount : " + totalCount + "   pageSize : " + pageSize);
            if (totalCount > pageSize) {
                System.out.println("실제 쿼리 : " + searchRequest.source().toString());
                System.out.println("######### next val ##### " + Arrays.toString( Arrays.stream(response.getHits().getHits())
                                                                                        .sorted(Comparator.comparing(SearchHit::getId))
//                                                                                 .reversed())
                                                                                        .findFirst()
                                                                                        .get()
                                                                                        .getSortValues()));
//                pageSize = totalCount > MAX_SEARCH_LIMIT ? MAX_SEARCH_LIMIT : totalCount;
                searchSourceBuilder
//                    .size(pageSize)
                    .searchAfter(Arrays.stream(response.getHits().getHits())
                                       .sorted(Comparator.comparing(SearchHit::getId))
//                                                         .reversed())
                                       .findFirst()
                                       .get()
                                       .getSortValues());
                System.out.println("###############조회 길이 : " +  response.getHits().getHits().length);
            }
            System.out.println(" loop 횟수: " + tmp);
            List<Map<String, Object>> searchList = extractDocuments(response);
            pivotIdList.addAll(searchList.stream().filter(e -> !pivotIdList.contains((String)e.get("es-id"))).map(e -> (String)e.get("es-id")).collect(Collectors.toList()));
            dupIdList.addAll(searchList.stream().filter(e -> pivotIdList.contains((String)e.get("es-id"))).map(e -> (String)e.get("es-id")).collect(Collectors.toList()));
            documents.addAll(searchList.stream().filter(e -> pivotIdList.contains((String)e.get("es-id"))).collect(Collectors.toList()));
            List<String> test = documents.stream().map(e -> (String)e.get("es-id")).collect(Collectors.toList());
            System.out.println("test : " + test);
            // TODO: 확인용
            System.out.println("######## documents size ########" + documents.size());
            System.out.println("pivotIdList size : " + pivotIdList.size() + " dupIdList size : " + dupIdList.size());

        }while (documents.size() != 0 && !(documents.size() >= totalCount));


        System.out.println("do 탈출 !!!!!");
        Cursor lastCursor;
        if (documents.isEmpty()) {
            lastCursor = Cursor.empty();
        } else {
            Map<String, Object> lastDocument = documents.get(documents.size() - 1);
            lastCursor = new Cursor(cursorField.read(lastDocument));
        }

        System.out.println("######## 최종 size ########" + documents.size());
        return new PageResult(index, documents, lastCursor);
    }

    private List<Map<String, Object>> extractDocuments(SearchResponse response) {
        return Arrays.stream(response.getHits().getHits())
                .map(hit -> {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    sourceMap.put("es-id", hit.getId());
                    sourceMap.put("es-index", hit.getIndex());
                    return sourceMap;
                }).collect(Collectors.toList());
    }

    public PageResult searchAfterWithSecondarySort(String index, Cursor cursor) throws IOException, InterruptedException {
        Objects.requireNonNull(secondaryCursorField);
        String primaryCursor = cursor.getPrimaryCursor();
        String secondaryCursor = cursor.getSecondaryCursor();
        boolean noPrevCursor = primaryCursor == null && secondaryCursor == null;

        QueryBuilder queryBuilder = noPrevCursor ? matchAllQuery() :
                getSecondarySortFieldQuery(primaryCursor, secondaryCursor);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .size(pageSize)
//                .sort(cursorSearchField, SortOrder.DESC)
//                .sort(secondaryCursorSearchField, SortOrder.DESC)
                .sort("_id", SortOrder.DESC)
                .trackTotalHits(true);

        SearchRequest searchRequest = new SearchRequest(index)
                .source(searchSourceBuilder);

        SearchResponse response = executeSearch(searchRequest);
        System.out.println("####### searchAfterWithSecondarySort #### response.totalhits : " + (int)response.getHits().getTotalHits().value);

        List<Map<String, Object>> documents = extractDocuments(response);

        Cursor lastCursor;
        if (documents.isEmpty()) {
            lastCursor = Cursor.empty();
        } else {
            Map<String, Object> lastDocument = documents.get(documents.size() - 1);
            String primaryCursorValue = cursorField.read(lastDocument);
            String secondaryCursorValue = secondaryCursorField.read(lastDocument);
            lastCursor = new Cursor(primaryCursorValue, secondaryCursorValue);
        }
        return new PageResult(index, documents, lastCursor);
    }

    private QueryBuilder buildGreaterThen(String cursorField, String cursorValue) {
//        return rangeQuery(cursorField).from(cursorValue, false);

        return termQuery("mod_dttm", "2021-03-03 01:22:02");
//        return rangeQuery(cursorField)
//            .from("2021-03-03 01:22:02", true)
//            .to("2021-03-03 01:22:02", true);
    }

    private QueryBuilder getSecondarySortFieldQuery(String primaryCursor, String secondaryCursor) {
        if (secondaryCursor == null) {
            return buildGreaterThen(cursorSearchField, primaryCursor);
        }
        return boolQuery()
                .minimumShouldMatch(1)
                .should(buildGreaterThen(cursorSearchField, primaryCursor))
                .should(
                        boolQuery()
                                .filter(matchQuery(cursorSearchField, primaryCursor))
                                .filter(buildGreaterThen(secondaryCursorSearchField, secondaryCursor))
                );
    }

    private SearchResponse executeSearch(SearchRequest searchRequest) throws IOException, InterruptedException {
        int maxTrials = elasticConnection.getMaxConnectionAttempts();
        if (maxTrials <= 0) {
            throw new IllegalArgumentException("MaxConnectionAttempts should be > 0");
        }
        IOException lastError = null;
        for (int i = 0; i < maxTrials; ++i) {
            try {
                return elasticConnection.getClient()
                        .search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                lastError = e;
                Thread.sleep(elasticConnection.getConnectionRetryBackoff());
            }
        }
        throw lastError;
    }

    public List<String> catIndices(String prefix) {
        Response resp;
        try {
            resp = elasticConnection.getClient()
                    .getLowLevelClient()
                    .performRequest(new Request("GET", "/_cat/indices"));
        } catch (IOException e) {
            logger.error("error in searching index names");
            throw new RuntimeException(e);
        }

        List<String> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String index = line.split("\\s+")[2];
                if (index.startsWith(prefix)) {
                    result.add(index);
                }
            }
        } catch (IOException e) {
            logger.error("error while getting indices", e);
        }

        Collections.sort(result);

        return result;
    }

    public void refreshIndex(String index) {
        try {
            elasticConnection.getClient()
                    .getLowLevelClient()
                    .performRequest(new Request("POST", "/" + index + "/_refresh"));
        } catch (IOException e) {
            logger.error("error in refreshing index " + index);
            throw new RuntimeException(e);
        }
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
