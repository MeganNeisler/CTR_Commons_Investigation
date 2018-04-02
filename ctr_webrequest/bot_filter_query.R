#Filter out ip address '115.29.47.109' when running query (likley a bot due to number of searches)
#Select dates where drop occured
start_date <- as.Date("2018-1-25")
end_date <- as.Date("2018-01-31")

daily_ctr_botfilter <- do.call(rbind, lapply(seq(start_date, end_date, "day"), function(date) {
  cat("Fetching webrequest data from ", as.character(date), "\n")
  clause_data <- wmf::date_clause(date)
  query <- paste("
                 ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
                 CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
                 CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
                 CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
                 CREATE TEMPORARY FUNCTION get_main_search_request AS 'org.wikimedia.analytics.refinery.hive.GetMainSearchRequestUDF';
                 
                 SELECT '", date, "' AS date, wiki, COUNT(search_requests.id) AS n_search, SUM(click) AS n_click
                 FROM (
                 SELECT DISTINCT id, wikiid AS wiki
                 from wmf_raw.cirrussearchrequestset",
                 clause_data$date_clause,
                 " and wikiid IN ('commonswiki', 'enwiki')
                 and source = 'web'
                 and backendusertests[0] IS NULL
                 -- Only take requests that include a full text search against the current wiki
                 -- (excludes completion suggester and other outliers).
                 AND get_main_search_request(wikiid, requests) IS NOT NULL
                 -- We only want 'normal' requests here. if the user requested more than
                 -- the default 20 results filter them out
                 AND SIZE(get_main_search_request(wikiid, requests).hits) <= 20
                 -- Excluding zero result searches
                 AND SIZE(get_main_search_request(wikiid, requests).hits) > 0
                 -- Excluding bots
                 and not (
                 ua_parser(useragent)['device_family'] = 'Spider'
                 OR is_spider(useragent)
                 OR ip IN ('127.0.0.1' , '115.29.47.109')
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
                 -- Collect web requests that have a searchToken in the referer (Users who disable javasript won't get a search token).
                 -- Including 'click', 'click to open in a new tab/window' and 'click on thumbnails to open mediaviewer'
                 SELECT DISTINCT PARSE_URL(referer, 'QUERY', 'searchToken') AS searchToken, '1' AS click
                 from wmf.webrequest",
                 clause_data$date_clause,
                 " AND webrequest_source = 'text'
                 AND (
                 normalized_host.project = 'commons' 
                 OR (
                 normalized_host.project = 'en'  
                 AND normalized_host.project_class = 'wikipedia'
                 )
                 )
                 and agent_type='user' 
                 and referer_class = 'internal'
                 and LENGTH(PARSE_URL(referer, 'QUERY', 'searchToken')) > 0
                 AND http_status IN('200', '304') 
                 AND (
                 -- click to pages
                 (
                 -- Users clicking navigational elements will match the search token,
                 -- but it won't be a pageview with page_id
                 is_pageview = TRUE
                 AND page_id IS NOT NULL
                 -- Don't include main page
            and page_id != 1 
            -- Don't include special page
                 and namespace_id != -1
                 )
                 -- click on thumbnails to open media viewer
                 OR (
                 uri_path = '/w/api.php'
                 and namespace_id = -1
                 and PARSE_URL(CONCAT('http://', uri_host, uri_path, uri_query), 'QUERY', 'prop') = 'imageinfo'
                 )
                 )
                 ) AS clickthroughs ON (search_requests.id=clickthroughs.searchToken)
                 GROUP BY wiki;") 
  results <- wmf::query_hive(query)
  return(results)
}))

readr::write_rds(daily_ctr_botfilter , "daily_ctr_botfilter.rds", "gz")

#LOCAL
system("scp mneisler@stat5:/home/mneisler/daily_ctr_botfilter.rds daily_ctr_botfilter.rds")



