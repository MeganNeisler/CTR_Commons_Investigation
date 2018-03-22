
#Joins full text search requests against webrequest logs to determine clickthrough rate on commons.

#Remotely from Stat005

#Get December 2017 to February 2018 ctr data on Commons
start_date <- as.Date("2017-12-18")
end_date <- as.Date("2018-02-28")

daily_ctr_Dec17 <- do.call(rbind, lapply(seq(start_date, end_date, "day"), function(date) {
  cat("Fetching webrequest data from ", as.character(date), "\n")
  clause_data <- wmf::date_clause(date)
  cat("test ", clause_data$date_clause)
  query <- paste("
        ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
        CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
        CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
        CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
        
        SELECT '", date, "' AS date, COUNT(search_requests.id) AS n_search, SUM(click) AS n_click
        FROM (
        SELECT DISTINCT id
        from wmf_raw.cirrussearchrequestset",
          clause_data$date_clause,
        " and wikiid='commonswiki'
        and source = 'web'
        and backendusertests[0] IS NULL
        and ARRAY_CONTAINS(requests.querytype, 'full_text')
        and NOT ARRAY_CONTAINS(requests.hitstotal, -1) 
        and array_sum(requests.hitstotal, -1) > 0 
        and not (
          ua_parser(useragent)['device_family'] = 'Spider'
          OR is_spider(useragent)
          OR ip = '127.0.0.1' 
          OR useragent RLIKE 'https?://'
          OR INSTR(useragent, 'www.') > 0
          OR INSTR(useragent, 'github') > 0
          OR LOWER(useragent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
          OR (
              ua_parser(useragent)['browser_family'] = 'Other'
              AND ua_parser(useragent)['device_family'] = 'Other'
              AND ua_parser(useragent)['os_family'] = 'Other'
              )
            )
        ) AS search_requests
        LEFT JOIN
        (
        SELECT DISTINCT PARSE_URL(referer, 'QUERY', 'searchToken') AS searchToken, '1' AS click
        from wmf.webrequest",
        clause_data$date_clause,
        " and normalized_host.project='commons'
        and agent_type='user' 
        and referer_class = 'internal'
        and namespace_id != -1 
        and page_id != 1 
        AND is_pageview  
        and LENGTH(PARSE_URL(referer, 'QUERY', 'searchToken')) > 0
        AND http_status IN('200', '304') 
        ) AS clickthroughs ON (search_requests.id=clickthroughs.searchToken);") 
  cat("test ", query)
  results <- wmf::query_hive(query)
  return(results)
}))


readr::write_rds(daily_ctr_Dec17, "daily_ctr_commons.rds", "gz")

#LOCAL
system("scp mneisler@stat5:/home/mneisler/daily_ctr_commons.rds daily_ctr_commons.rds")


#Query EnWiki and Commons Data for comparison
start_date <- as.Date("2017-12-22")
end_date <- as.Date("2018-02-28")

daily_ctr_commons_enwiki <- do.call(rbind, lapply(seq(start_date, end_date, "day"), function(date) {
  cat("Fetching webrequest data from ", as.character(date), "\n")
  clause_data <- wmf::date_clause(date)
  query <- paste("
                 ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
                 CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
                 CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
                 CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
                 
                 SELECT '", date, "' AS date, wiki, COUNT(search_requests.id) AS n_search, SUM(click) AS n_click
                 FROM (
                 SELECT DISTINCT id, wikiid AS wiki
                 from wmf_raw.cirrussearchrequestset",
                 clause_data$date_clause,
                 " and wikiid IN ('commonswiki', 'enwiki')
                 and source = 'web'
                 and backendusertests[0] IS NULL
                 and ARRAY_CONTAINS(requests.querytype, 'full_text')
                 and NOT ARRAY_CONTAINS(requests.hitstotal, -1)
                 and array_sum(requests.hitstotal, -1) > 0 -- non zero result
                 -- not bot
                 and not (
                 ua_parser(useragent)['device_family'] = 'Spider'
                 OR is_spider(useragent)
                 OR ip IN ('127.0.0.1', '115.29.47.109')  --remove identified bot ips
                 OR useragent RLIKE 'https?://'
                 OR INSTR(useragent, 'www.') > 0
                 OR INSTR(useragent, 'github') > 0
                 OR LOWER(useragent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
                 OR (
                 ua_parser(useragent)['browser_family'] = 'Other'
                 AND ua_parser(useragent)['device_family'] = 'Other'
                 AND ua_parser(useragent)['os_family'] = 'Other'
                 )
                 )
                 ) AS search_requests
                 LEFT JOIN
                 (
                 SELECT DISTINCT PARSE_URL(referer, 'QUERY', 'searchToken') AS searchToken, '1' AS click
                 from wmf.webrequest",
                 clause_data$date_clause,
                 " and normalized_host.project IN ('commons', 'en')
                 and agent_type='user'
                 and referer_class = 'internal'
                 and namespace_id != -1
                 and page_id != 1
                 AND is_pageview 
                 and LENGTH(PARSE_URL(referer, 'QUERY', 'searchToken')) > 0
                 AND http_status IN('200', '304')
                 ) AS clickthroughs ON (search_requests.id=clickthroughs.searchToken)
                 GROUP BY wiki;") 
  cat("test ", query)
  results <- wmf::query_hive(query)
  return(results)
}))

readr::write_rds(daily_ctr_commons_enwiki, "daily_ctr_commons_enwiki.rds", "gz")

#LOCAL
system("scp mneisler@stat5:/home/mneisler/daily_ctr_commons_enwiki.rds daily_ctr_commons_enwiki.rds")

## Determine number of search clicks by namespace looking at top 3 namespaces (0, 6 and 14)
#Remotely from Stat 5

start_date <- as.Date("2017-12-20")
end_date <- as.Date("2018-1-30")

daily_clicks_bynamespace <- do.call(rbind, lapply(seq(start_date, end_date, "day"), function(date) {
  cat("Fetching webrequest data from ", as.character(date), "\n")
  clause_data <- wmf::date_clause(date)
  query <- paste("
                 ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
                 CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
                 CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
                 CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
                 
                 SELECT '", date, "' AS date, namespace, SUM(click) AS n_click
                 FROM (
                 SELECT DISTINCT id
                 from wmf_raw.cirrussearchrequestset",
                 clause_data$date_clause,
                 " and wikiid = 'commonswiki'
                 and source = 'web'
                 and backendusertests[0] IS NULL
                 and ARRAY_CONTAINS(requests.querytype, 'full_text')
                 and NOT ARRAY_CONTAINS(requests.hitstotal, -1)
                 and array_sum(requests.hitstotal, -1) > 0 -- non zero result
                 -- not bot
                 and not (
                 ua_parser(useragent)['device_family'] = 'Spider'
                 OR is_spider(useragent)
                 OR ip = '127.0.0.1'
                 OR useragent RLIKE 'https?://'
                 OR INSTR(useragent, 'www.') > 0
                 OR INSTR(useragent, 'github') > 0
                 OR LOWER(useragent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
                 OR (
                 ua_parser(useragent)['browser_family'] = 'Other'
                 AND ua_parser(useragent)['device_family'] = 'Other'
                 AND ua_parser(useragent)['os_family'] = 'Other'
                 )
                 )
                 ) AS search_requests
                 LEFT JOIN
                 (
                 SELECT DISTINCT PARSE_URL(referer, 'QUERY', 'searchToken') AS searchToken, '1' AS click, namespace_id AS namespace
                 from wmf.webrequest",
                 clause_data$date_clause,
                 " and normalized_host.project = 'commons'
                 and agent_type='user'
                 and referer_class = 'internal'
                 and namespace_id IN (0, 6, 14)
                 and page_id != 1
                 AND is_pageview 
                 and LENGTH(PARSE_URL(referer, 'QUERY', 'searchToken')) > 0
                 AND http_status IN('200', '304')
                 ) AS clickthroughs ON (search_requests.id=clickthroughs.searchToken)
                 GROUP BY namespace;") 
  cat("test ", query)
  results <- wmf::query_hive(query)
  return(results)
}))

readr::write_rds(daily_clicks_bynamespace, "daily_clicks_bynamespace.rds", "gz")

#LOCAL
system("scp mneisler@stat5:/home/mneisler/daily_clicks_bynamespace.rds daily_clicks_bynamespace.rds")

#Find robot leading to decline in clickthroughs between Jan 23 and Jan 28
start_date <- as.Date("2018-01-27")
end_date <- as.Date("2018-01-27")

daily_searches_byuser <- do.call(rbind, lapply(seq(start_date, end_date, "day"), function(date) {
  cat("Fetching webrequest data from ", as.character(date), "\n")
  clause_data <- wmf::date_clause(date)
  cat("test ", clause_data$date_clause)
  query <- paste("
                 ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
                 CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
                 CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
                 CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
                 
                 SELECT '", date, "' AS date, COUNT(search_requests.id) AS n_search, useragent, ip
                 FROM
                 (
                 SELECT DISTINCT id, useragent, ip
                 from wmf_raw.cirrussearchrequestset",
                 clause_data$date_clause,
                 " and wikiid='commonswiki'
                 and source = 'web'
                 and backendusertests[0] IS NULL
                 and ARRAY_CONTAINS(requests.querytype, 'full_text')
                 and NOT ARRAY_CONTAINS(requests.hitstotal, -1)
                 and array_sum(requests.hitstotal, -1) > 0 -- non zero result
                 and ip != '115.29.47.109'
                 -- not bot
                 and not (
                 ua_parser(useragent)['device_family'] = 'Spider'
                 OR is_spider(useragent)
                 OR ip = '127.0.0.1'
                 OR useragent RLIKE 'https?://'
                 OR INSTR(useragent, 'www.') > 0
                 OR INSTR(useragent, 'github') > 0
                 OR LOWER(useragent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
                 OR (
                 ua_parser(useragent)['browser_family'] = 'Other'
                 AND ua_parser(useragent)['device_family'] = 'Other'
                 AND ua_parser(useragent)['os_family'] = 'Other'
                 )
                 )
                 ) AS search_requests
                 GROUP BY useragent, ip;") 
  
  cat("test", query)
  results <- wmf::query_hive(query)
  return(results)
}))


readr::write_rds(daily_searches_byuser, "daily_searches_byuser.rds", "gz")

#LOCAL
system("scp mneisler@stat5:/home/mneisler/daily_searches_byuser.rds daily_searches_byuser.rds")

