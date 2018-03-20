
#Review of user distribution and behavior around December 14th.
#Using full_desktop_data_Nov17Feb18.Rdata

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


# Review count of users that have not clicked on SERPS.
fig_path <- file.path("figures")
plot_resolution <- 192


#Plot search activity around decline date.

#Aggregating by search (Keeping sessions with over 50 searches)
searches <- events %>%
        keep_where(!(is.na(search_id))) %>% # remove visitPage and checkin events
        arrange(date, session_id, search_id, timestamp) %>%
        group_by(session_id, search_id) %>%
        summarize(
                date = date[1],
                timestamp = timestamp[1],
                `event scroll` = sum(n_scroll_serp, na.rm = TRUE) > 0,
                `got same-wiki results` = any(`some same-wiki results` == "TRUE", na.rm = TRUE),
                engaged = any(event != "searchResultPage") || length(unique(page_id[event == "searchResultPage"])) > 1  || any(`event scroll`),
                `same-wiki clickthrough` = "click" %in% event,
                `no. same-wiki results clicked` = length(unique(event_position[event == "click"])),
                `first clicked same-wiki results position` = ifelse(`same-wiki clickthrough`, event_position[event == "click"][1], NA), # event_position is 0-based
                `max clicked position (same-wiki)` = ifelse(`same-wiki clickthrough`, max(event_position[event == "click"], na.rm = TRUE), NA)
        ) %>%
        ungroup

 #Plot of User Behavior on SERP

#Broken down by number of searches
 p <- searches %>%
         filter(date <= '2018-2-28')%>% #remove last day to incomplete data
         group_by(date) %>%
         summarize( 
                 `All searches` = n(),
                 `All Search sessions (under 50 searches)` = length(unique(session_id)),
                 `Searches with results` = sum(`got same-wiki results`),
                 `Searches with clicks` = sum(`same-wiki clickthrough`),
                 `Searches with no clicks` = sum(!`same-wiki clickthrough`),
                 `Any event scroll` = sum(`event scroll`)
         ) %>%
         gather(key = Type, value = count, -date) %>%
         ggplot(aes(x = date, y = count, colour = Type)) +
         geom_line(size = 1.2) +
         geom_vline(xintercept = as.numeric(as.Date("2017-12-14")), linetype="dotted", 
                    color = "black") +
         geom_text(aes(x=as.Date('2017-12-14'), y=800, label="Change 398394 Deploy Date"), size=3, vjust = -1.2, angle = 90, color = "black") +
         scale_x_date(name = "Date") +
         scale_y_continuous(labels = polloi::compress, name = "Count") +
         labs(title = "Daily desktop full-text search activity on Commons",
              subtitle = "December 2017 to February 2018") +
         wmf::theme_min()
 
 ggsave("daily_searches.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
 rm(p)
 
 
 #Brokend down by number of sessions

 
 #Review SERP offset activity around decline date
 p <- serp_offset %>% #remove last day to incomplete data
         group_by(session_id, search_id) %>%
         summarize(`Any page-turning` = any(offset > 0)) %>%
         dplyr::right_join(searches, by = c("session_id", "search_id")) %>%
         #review time around decline and remove pages withclick
         group_by(date) %>%
         mutate(page_turn = sum(`Any page-turning`, na.rm = TRUE), n_search = n()) %>%
         ungroup() %>%
         cbind(
                 as.data.frame(binom:::binom.bayes(x = .$page_turn, n = .$n_search, conf.level = 0.95, tol = 1e-10))
         ) %>%
         ggplot(aes(x = date, y = mean, ymin = lower, ymax = upper)) +
         geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.1, color = NA) +
         geom_line() +
         geom_vline(xintercept = as.numeric(as.Date("2017-12-14")), linetype="dotted", 
                    color = "black") +
         geom_text(aes(x=as.Date('2017-12-14'), y=.12, label="Change 398394 Deploy Date"), size=3, vjust = -1.2, angle = 90, color = "black") +
         scale_y_continuous("Proportion of searches", labels = scales::percent_format()) +
         labs(title = "Proportion of desktop full-text searches with clicks to see other pages of the search results") +
         wmf::theme_min(plot.title = element_text(size=13))
 
 ggsave("daily_serp_offset.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
 rm(p)
 
 # Check the log of several sessions with no click, is there any pattern
 
eventlogs_dec14  <- events %>% 
        filter(date == '2017-12-14')%>% 
        group_by(date) %>% 
        filter(!("click" %in% event)) %>% 
        select(date, session_id, search_id, event_id, event, event_position, `some same-wiki results`, n_results, event_scroll, event_checkin, load_time)
        
write.csv(eventlogs_dec14, "eventlogs_dec14.csv")
 
 #Before and after the drop, does the distribution of the users (in terms of browser and operating systems) change a lot?
 

 user_agents <- events %>%
         distinct(date, session_id, user_agent)
 
 user_agents <- user_agents %>%
         #focus on days of the decline.
         cbind(., purrr::map_df(.$user_agent, ~ wmf::null2na(jsonlite::fromJSON(.x, simplifyVector = FALSE)))) %>%
         mutate(
                 browser = paste(browser_family, browser_major),
                 os = case_when(
                         is.na(os_major) ~ os_family,
                         !is.na(os_major) & !is.na(os_minor) ~ paste0(os_family, " ", os_major, ".", os_minor),
                         TRUE ~ paste(os_family, os_major)
                 )
         )
         
#Look at OS distribution
 library(lubridate)
 top_5_oses <- names(head(sort(table(user_agents$os), decreasing = TRUE), 5))
 os_summary <- user_agents %>%
         filter(date >= '2017-12-10' & date <= '2017-12-23') %>%
         mutate(os = if_else(os %in% top_5_oses, os, "Other OSes")) %>%
         mutate(week = floor_date(date, "week", week_start = getOption("lubridate.week.start", 7))) %>%  #aggregate by week
         group_by(week, os) %>%
         tally %>%
         mutate(Proportion = (n / sum(n))) 
 
 
 p <- os_summary %>%
         ggplot(aes(x = os, y = Proportion, fill = os)) +
         geom_col(position = "dodge") +
         scale_y_continuous(labels = polloi::compress, name = "Proportion of Sessions") +
         facet_wrap (~ week, scale = "free_y") + 
         labs(title = "Daily desktop full-text searches on Commons by Os type",
              subtitle = "Week before and after Dec 14th") +
         theme(axis.text.x = element_text(angle = 60, hjust = 1)) 
 
 ggsave("daily_searches_byoses.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
 rm(p)
 

 
 #Look at top 5 browsers
 top_5_browsers <- names(head(sort(table(user_agents$browser), decreasing = TRUE), 5))
 browser_summary <- user_agents %>%
         filter(date >= '2017-12-10' & date <= '2017-12-23') %>%
         mutate(browser = if_else(browser %in% top_5_browsers, browser, "Other browsers")) %>%
         mutate(week = floor_date(date, "week", week_start = getOption("lubridate.week.start", 7))) %>%  #aggregate by week
         group_by(week, browser) %>%
         tally %>%
         mutate(Proportion = (n / sum(n))) 

 
 p <- browser_summary %>%
         ggplot(aes(x = browser, y = Proportion, fill = browser)) +
         geom_col(position = "dodge") +
         scale_y_continuous(labels = polloi::compress, name = "Proportion of Sessions") +
         facet_wrap (~ week, scale = "free_y") +
         labs(title = "Daily desktop full-text searches on Commons by browser type",
              subtitle = "Week before and after Dec 14th") +
         theme(axis.text.x = element_text(angle = 60, hjust = 1)) 
 
 ggsave("daily_searches_bybrowser.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
 rm(p)


 