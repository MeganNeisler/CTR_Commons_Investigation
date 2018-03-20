message("Create an auto-closing SSH tunnel in the backgroud")
system("ssh -f -o ExitOnForwardFailure=yes stat1006.eqiad.wmnet -L 3307:analytics-slave.eqiad.wmnet:3306 sleep 10")
library(RMySQL)
con <- dbConnect(MySQL(), host = "127.0.0.1", group = "client", dbname = "log", port = 3307)

#Filtered and cleaned dataset
#query testsatisfaction dataset to obtain full text desktop search events on Commons from November 2017 and 2018.
query <- "
SELECT
        timestamp,
        wiki,
        event_uniqueId AS event_id,
        event_pageViewId AS page_id,
        event_articleId AS article_id,
        event_searchSessionId AS session_id,
        MD5(LOWER(TRIM(event_query))) AS query_hash,
        event_action AS event,
        CASE
        WHEN event_position < 0 THEN NULL
        ELSE event_position
        END AS event_position,
        CASE
        WHEN event_action = 'searchResultPage' AND event_hitsReturned > 0 THEN 'TRUE'
        WHEN event_action = 'searchResultPage' AND event_hitsReturned IS NULL THEN 'FALSE'
        ELSE NULL
        END AS `some same-wiki results`,
        CASE
        WHEN event_action = 'searchResultPage' AND event_hitsReturned > -1 THEN event_hitsReturned
        WHEN event_action = 'searchResultPage' AND event_hitsReturned IS NULL THEN 0
        ELSE NULL
        END AS n_results,
        event_scroll,
        event_checkin,
        event_extraParams,
        event_msToDisplayResults AS load_time,
        userAgent AS user_agent
FROM TestSearchSatisfaction2_16909631
WHERE wiki = 'commonswiki'
AND (LEFT(timestamp, 6) BETWEEN '201711' AND '201802')
AND INSTR(userAgent, '\"is_bot\": false') > 0
AND event_source = 'fulltext'
AND event_subTest IS NULL
AND CASE WHEN event_action = 'searchResultPage' THEN event_msToDisplayResults IS NOT NULL
        WHEN event_action IN ('click', 'iwclick', 'ssclick') THEN event_position IS NOT NULL AND event_position > -1
        WHEN event_action = 'visitPage' THEN event_pageViewId IS NOT NULL
        WHEN event_action = 'checkin' THEN event_checkin IS NOT NULL AND event_pageViewId IS NOT NULL
        ELSE TRUE
        END;"

fulltext_events_commons_searchquery<- wmf::mysql_read(query, "log", con)
wmf::mysql_disconnect(con)
save(fulltext_events_commons_searchquery , file = "data/fulltext_events_commons_searchquery.RData")

#load required packages
library(magrittr)
library(tidyverse)
library(ggplot2)
library(Rcpp)
import::from(
        dplyr,
        keep_where = filter, select,
        group_by, ungroup,
        mutate, arrange, summarize, tally,
        case_when, if_else
)

# Clean Data

load("data/fulltext_events_commons_searchquery.RData")
events <- fulltext_events_commons_searchquery
#De-duplicating events
events <- events %>%
        mutate(
                timestamp = lubridate::ymd_hms(timestamp),
                date = as.Date(timestamp)
        ) %>%
        arrange(session_id, event_id, timestamp) %>%
        dplyr::distinct(session_id, event_id, .keep_all = TRUE)
rm(fulltext_events_commons_searchquery) 

##Filtered Dataset
# Sum all scroll check-in events and remove unnecessary check-ins.
events <- events %>%
        group_by(session_id, page_id) %>%
        mutate(event_scroll = ifelse(event == "checkin", sum(event_scroll), event_scroll)) %>% # sum all scroll on visitPage and checkin events
        ungroup

events <- events[order(events$session_id, events$page_id, events$article_id, events$event, events$event_checkin, na.last = FALSE), ]
#remove extra check-ins
extra_checkins <- duplicated(events[, c("session_id", "page_id", "article_id", "event")], fromLast = TRUE) & events$event == "checkin"
events <- events[!extra_checkins, ]
rm(extra_checkins)

#Delete events with negative load time
events <- events %>%
        keep_where(is.na(load_time) | load_time >= 0)

#De-duplicating SERPs
SERPs <- events %>%
        keep_where(event == "searchResultPage") %>%
        arrange(session_id, timestamp) %>%
        select(c(session_id, page_id, query_hash)) %>%
        group_by(session_id, query_hash) %>%
        mutate(search_id = page_id[1]) %>%
        ungroup %>%
        select(c(session_id, page_id, search_id))

events <- events %>%
        dplyr::left_join(SERPs, by = c("session_id", "page_id"))
rm(SERPs) 

# Removing events without an associated SERP (orphan clicks and check-ins)
n_event <- nrow(events)
events <- events %>%
        keep_where(!(is.na(search_id) & !(event %in% c("visitPage", "checkin")))) %>% # remove orphan click
        group_by(session_id) %>%
        keep_where("searchResultPage" %in% event) %>% # remove orphan "visitPage" and "checkin"
        ungroup
rm(n_event)

#Removing sessions with more than 50 searches

spider_session <- events %>%
        group_by(date, session_id) %>%
        summarize(n_search = length(unique(search_id))) %>%
        keep_where(n_search > 50) %>%
        {.$session_id}
events <- events %>%
        keep_where(!(session_id %in% spider_session))
rm(spider_session)

#Check scroll on SERPs. 
events <- events %>%
        keep_where(!(event %in% c("visitPage", "checkin"))) %>%
        group_by(session_id, page_id) %>%
        summarize(n_scroll_serp = sum(event_scroll)) %>%
        ungroup %>%
        dplyr::right_join(events, by = c("session_id", "page_id"))



#Processing SERP offset data..."
parse_extraParams <- function(extraParams, action){
        if (extraParams == "{}") {
                if (all(action %in% c("hover-on", "hover-off"))) {
                        return(list(hoverId = NA, section = NA, results = NA))
                } else if (all(action %in% c("esclick"))) {
                        return(list(hoverId = NA, section = NA, result = NA))
                } else if (all(action %in% c("searchResultPage"))) {
                        return(list(offset = NA, iw = list(source = NA, position = NA)))
                } else {
                        return(NA)
                }
        } else {
                if (all(action %in% c("searchResultPage"))) {
                        output <- jsonlite::fromJSON(txt = as.character(extraParams), simplifyVector = TRUE)
                        offset <- polloi::data_select(is.null(output$offset), NA, output$offset)
                        iw <- polloi::data_select(is.null(output$iw), list(source = NA, position = NA), output$iw)
                        return(list(offset = offset, iw = iw))
                } else {
                        # "hover-on", "hover-off", "esclick"
                        return(jsonlite::fromJSON(txt = as.character(extraParams), simplifyVector = TRUE))
                }
        }
}
serp_offset <- events %>%
        keep_where(event == "searchResultPage", `some same-wiki results` == "TRUE") %>%
        # SERPs with 0 results will not have an offset in extraParams ^
        mutate(offset = purrr::map_int(event_extraParams, ~ parse_extraParams(.x, action = "searchResultPage")$offset)) %>%
        select(session_id, event_id, search_id, offset)


