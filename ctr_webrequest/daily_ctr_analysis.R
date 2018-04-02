#load required packages
library(tidyverse)
library(ggplot2)
library(scales)
library(magrittr)
library(reshape)
import::from(
  dplyr,
  keep_where = filter, 
  group_by, ungroup,
  mutate
)

fig_path <- file.path("figures")
plot_resolution <- 192

#Daily CTR on English Wikipedia and Commons. 

ctr_commons_enwiki <- rbind(readr::read_rds("data/daily_ctr_commons_enwiki.rds")) %>%
  mutate(date = lubridate::ymd(date)) 

ctr_commons_enwiki$wiki <- ifelse(ctr_commons_enwiki$wiki == "enwiki", "English Wikipedia", "Commons")

p <- ctr_commons_enwiki %>%
  cbind(
    as.data.frame(binom:::binom.bayes(x = .$n_click, n = .$n_search, conf.level = 0.95, tol = 1e-9))
  ) %>%
  ggplot(aes(x = date, color = wiki, y = mean, ymin = lower, ymax = upper)) +
  geom_line() +
  scale_color_brewer("Wiki", palette = "Set1") +
  scale_fill_brewer("Wiki", palette = "Set1") +
  scale_y_continuous("Clickthrough rate", labels = scales::percent_format()) +
  scale_x_date(labels = date_format("%d-%b-%y"), date_breaks = "1 week") +
  labs(title = "Daily full-text clickthrough rates on desktop", subtitle = "From webrequest and cirrusesearchrequest data") +
  wmf::theme_min()
ggsave("daily_ctr_commons_enwiki.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)


#Look at searchess grouped by user (ip and useragent) to find potential bot leading to CTR decline between Jan 23, 2018 and Jan 31, 2018.

daily_searches_byuser <- rbind(readr::read_rds("data/daily_searches_byuser.rds")) %>%
  mutate(date = lubridate::ymd(X_c0),
         searches = as.numeric(n_search)) %>%
  filter(searches > 3000)

#Shows ip address run on Jan 25 to Jan 29th over 10000 searches a day. 
#CTR Query was then rerun filtering out identified ip address (bot_filter_query.R)
#Replace n_search values for Jan 25 to Jan 29 dates with filtered data.

daily_ctr_botfilter <- rbind(readr::read_rds("data/daily_ctr_botfilter.rds")) %>%
  mutate(date = lubridate::ymd(date))

ctr_commons_enwiki$n_search <- replace(ctr_commons_enwiki$n_search, 
                                       ctr_commons_enwiki$date >= '2018-01-25' & ctr_commons_enwiki$date <= '2018-01-31', 
                                       daily_ctr_botfilter$n_search)
  
#Redo plot using filtered data
ctr_commons_enwiki_filtered <- ctr_commons_enwiki%>%
  cbind(
    as.data.frame(binom:::binom.bayes(x = .$n_click, n = .$n_search, conf.level = 0.95, tol = 1e-9))
  ) %>%
  ggplot(aes(x = date, color = wiki, y = mean, ymin = lower, ymax = upper)) +
  geom_ribbon(aes(ymin = lower, ymax = upper, fill = wiki), alpha = 0.1, color = NA) +
  geom_line() +
  scale_color_brewer("Wiki", palette = "Set1") +
  scale_fill_brewer("Wiki", palette = "Set1") +
  scale_y_continuous("Clickthrough rate", labels = scales::percent_format()) +
  scale_x_date(labels = date_format("%d-%b-%y"), date_breaks = "1 week") +
  labs(title = "Daily full-text clickthrough rates on desktop", 
       subtitle = "January 1, 2018 to March 10,2018",
       caption = "From webrequest and cirrusesearchrequest data. Data filtered to remove suspected bots") +
  wmf::theme_min()
ggsave("daily_ctr_commons_enwiki_filtered.png", ctr_commons_enwiki_filtered, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(ctr_commons_enwiki_filtered)

  
  
#CTR by namespace on Commons
##Reshape data
daily_ctr_bynamespace <- rbind(readr::read_rds("data/daily_ctr_bynamespace.rds")) %>%
  mutate(date = lubridate::ymd(date)) %>%
  melt(id.vars = c("date", "n_search"), variable_name = "namespace")

daily_ctr_bynamespace$namespace %<>% factor(c('click_on_ns0', 'click_on_ns6','click_on_ns14'), c("Main Article", "File", "Category"))


#Plot ctr by namespace on commons 
p <- daily_ctr_bynamespace %>%
  cbind(
    as.data.frame(binom:::binom.bayes(x = .$value, n = .$n_search, conf.level = 0.95, tol = 1e-9))
  ) %>% 
  ggplot(aes(x = date, color = namespace, y = mean, ymin = lower, ymax = upper)) +
  geom_line() +
  scale_color_brewer("namespace", palette = "Set1") +
  scale_x_date(labels = date_format("%d-%b-%y"), date_breaks = "1 week") +
  scale_y_continuous(trans= "log", name = "Clickthrough rate (log scale)", labels = scales::percent_format()) +
  scale_color_brewer("Namespace", palette = "Set1") +
  labs(title = "Daily full-text search clickthrough rate on desktop on Wikimedia Common by namespace", 
       subtitle = "January 1, 2018 to March 10,2018",
       caption = "From webrequest and cirrusesearchrequest data") +
  wmf::theme_min()

ggsave("daily_ctr_bynamespace.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)



