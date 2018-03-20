
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

fig_path <- file.path("figures")
plot_resolution <- 192

#Load data queried from webrequest and cirrusserachrequestset data
daily_ctr_commons <- rbind(readr::read_rds("data/daily_ctr_commons.rds")) %>%
  mutate(date = lubridate::ymd(date))  %>%
  cbind(
    as.data.frame(binom:::binom.bayes(x = .$n_click, n = .$n_search, conf.level = 0.95, tol = 1e-9))
  )


#Plot daily ctr on commons from Dec 17 through Feb 18
p <-  daily_ctr_commons %>%
  ggplot(aes(x = date, y = mean, ymin = lower, ymax = upper)) +
  geom_ribbon(aes(ymin = lower, ymax = upper), alpha = 0.1, fill = "red") +
  geom_line(color ="red") +
  geom_hline(aes(yintercept = median(mean)), linetype = "dashed") +
  scale_y_continuous("clickthrough rate", labels = scales::percent_format()) +
  scale_x_date(labels = date_format("%d-%b-%y"), date_breaks = "1 week") +
  labs(title = "Daily search-wise full-text clickthrough rates on desktop on Wikimedia Commons", 
       subtitle = "From webrequest and cirrusesearchrequestset data", 
       caption = "The dashed line marks the median proportion of files deleted within 1 month") +
  wmf::theme_min()

ggsave("daily_ctr_commons.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)

#The overall ctr is higher than found using eventlogging; however, there is sudden drop the last week in January with it's lowest point on Jan 28th.
#I reviewed the ctr for both COmmons and English Wikipedia in January to determine if this drop was associated with just Commons.

#CTR Comparison between English Wikipedia and Commons

ctr_commons_enwiki <- rbind(readr::read_rds("data/daily_ctr_commons_enwiki.rds")) 

ctr_commons_enwiki$wiki <- ifelse(ctr_commons_enwiki$wiki == "enwiki", "English Wikipedia", "Commons")

p <- ctr_commons_enwiki %>%
  mutate(date = lubridate::ymd(date)) %>%
  group_by(wiki, date) %>%
  ungroup %>%
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
  labs(title = "Daily search-wise full-text clickthrough rates on desktop", subtitle = "From webrequest and cirrusesearchrequest data") +
  wmf::theme_min()
ggsave("daily_ctr_commons_enwiki.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)

#File vs Article search clicks on Commons

daily_clicks_bynamespace <- rbind(readr::read_rds("data/daily_clicks_bynamespace.rds")) %>%
  mutate(date = lubridate::ymd(date),
         clicks = as.numeric(n_click)) %>%
  filter(namespace != 'NULL',
         n_click != 'NULL')

daily_clicks_bynamespace$namespace %<>% factor(c(0, 6, 14), c("Main Article", "File", "Category"))

#Plot total clicks on commons 
p <-  daily_clicks_bynamespace %>%
  ggplot(aes(x = date, y = clicks, color = namespace)) +
  geom_line() +
  scale_x_date(labels = date_format("%d-%b-%y"), date_breaks = "1 week") +
  scale_y_continuous("Total daily search clicks") +
  scale_color_brewer("Namespace", palette = "Set1") +
  labs(title = "Daily full-text search clicks on desktop on Wikimedia Common by namespace") +
  wmf::theme_min()

ggsave("daily_clicks_bynamespace.png", p, path = fig_path, units = "in", dpi = plot_resolution, height = 6, width = 10, limitsize = FALSE)
rm(p)


