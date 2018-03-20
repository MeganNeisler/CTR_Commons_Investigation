message("Create an auto-closing SSH tunnel in the backgroud")
system("ssh -f -o ExitOnForwardFailure=yes stat1006.eqiad.wmnet -L 3307:analytics-slave.eqiad.wmnet:3306 sleep 10")
library(RMySQL)
con <- dbConnect(MySQL(), host = "127.0.0.1", group = "client", dbname = "log", port = 3307)

#Investigate the full-text desktop search CTR decline on Wikimedia Commons

query <- "
SELECT
        LEFT(timestamp, 8) AS date,
        timestamp,
        wiki,
        event_uniqueId AS event_id,
        event_searchSessionId AS session_id,
        userAgent AS user_agent,
        MD5(LOWER(TRIM(event_query))) AS query_hash,
        event_pageViewId AS page_id,
        event_action AS action,
        event_checkin AS checkin,
        CASE
        WHEN event_position < 0 THEN NULL
        ELSE event_position
        END AS event_position,
        event_hitsReturned AS results_returned
        FROM TestSearchSatisfaction2_16909631
        WHERE wiki = 'commonswiki'
        AND (LEFT(timestamp, 6) BETWEEN '201711' AND '201802')
        AND INSTR(userAgent, '\"is_bot\": false') > 0
        AND event_source = 'fulltext'
        AND event_action IN('searchResultPage', 'click')
        AND (event_subTest IS NULL OR event_subTest IN ('null', 'baseline'));
"

fulltext_events_commons_raw <- wmf::mysql_read(query, "log", con)
wmf::mysql_disconnect(con)
save(fulltext_events_commons_raw, file = "data/fulltext_events_commons_raw.RData")

#load required packages
library(magrittr)
library(tidyverse)
library(ggplot2)
library(Rcpp)
library(scales)
import::from(
        dplyr,
        keep_where = filter, select,
        group_by, ungroup,
        mutate, arrange, summarize, tally,
        case_when, if_else
)

load("data/fulltext_events_commons_raw.RData")
fulltext_events_commons <- fulltext_events_commons_raw
fulltext_events_commons$timestamp <- as.POSIXct(fulltext_events_commons$timestamp, format = "%Y%m%d%H%M%S")
fulltext_events_commons <- fulltext_events_commons[order(fulltext_events_commons$event_id, fulltext_events_commons$timestamp), ]
fulltext_events_commons <- fulltext_events_commons[!duplicated(fulltext_events_commons$event_id, fromLast = TRUE), ]
fulltext_events_commons <- fulltext_events_commons[order(fulltext_events_commons$session_id, fulltext_events_commons$page_id, fulltext_events_commons$timestamp), ]


#Find daily full-text search-wise CTR for Commons from November to now.

daily_ctr_commons_bysearch <-  fulltext_events_commons %>%
        filter(date < "20180228") %>% #remove data from last day due to incomplete data on that day.
        mutate(date = lubridate::ymd(date)) %>%
        group_by(date, session_id, page_id) %>%
        summarize(
                clickthrough = all(c("searchResultPage", "click") %in% action)) %>%
        group_by(date) %>%
        summarize(
                ctr = mean(clickthrough)) %>%
        gather(type, ctr, -date) %>%
        mutate(platform = "Desktop")
   

#Find session-wise CTR, total clicks / total impressions). Do we see the same pattern?

daily_ctr_commons_sessions <- fulltext_events_commons %>%
        filter(date < "20180228") %>% #remove data from last day due to incomplete data on that day.
        mutate(date = lubridate::ymd(date)) %>%
        group_by(date, session_id) %>%
        summarize(
                max_results = max(results_returned, na.rm = TRUE),
                clicked = any(action == "click")
        ) %>%
        ungroup() %>%
        filter(max_results > 0) %>%
        group_by(date) %>%
        summarize(
                ctr = mean(clicked, na.rm = TRUE)
        ) %>%
        ungroup() %>%
        mutate(platform = "Desktop")
                            
#Plot daily session and search-wise CTR rates from November 2017 to Feburary 2018.

fig_path <- file.path("figures")
plot_resolution <- 192


p <-  
        ggplot() +
        geom_line(data = daily_ctr_commons_sessions, aes(x = date, y = ctr, color = "blue"), show.legend = TRUE) +
        geom_line(data = daily_ctr_commons_bysearch, aes(x= date, y= ctr,  color = "red"), show.legend = TRUE) +
        scale_y_continuous("clickthrough rate", labels = scales::percent_format()) +
        scale_x_date(labels = date_format("%d-%b"), date_breaks = "1 week") +
        geom_vline(xintercept = as.numeric(as.Date("2017-12-14")), linetype="dotted", 
                   color = "black") + 
        scale_color_discrete(name = "Metrics", labels=c("Session-wise CTR", "Search-wise CTR")) +
        geom_text(aes(x=as.Date('2017-12-14'), y=.12, label="Change 398394 Deploy Date"), size=3, vjust = -1.2, angle = 90) +
        labs(title = "Daily full-text clickthrough rates on desktop on Wikimedia Commons", 
             subtitle = "November 2017 through February 2018", 
             caption = "*clickthrough rates = total clicks / total search result pages.") +
        wmf::theme_min()

ggsave("daily_ctr_commons_Nov17Feb18.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)


