library(data.table)
library(dplyr)
library(prophet)
library(RMySQL)

#Get data from MySQL
suppress_Warnings()
#disconnect
#dbDisconnect(comdb) 
#Clear Resultset
dbClearResult(dbListResults(comdb)[[1]])
#establish connection to ComTrak
comdb = dbConnect(MySQL(), user='tganka', password='tganka_123!', dbname='comtrak', host='172.16.122.33')
#Cohort Spread
db.co = dbSendQuery(comdb, "
                    SELECT
                    CASE 
                    WHEN cohort <= 2013 THEN '2013'
                    WHEN cohort = 2014 THEN '2014'
                    WHEN cohort = 2015 THEN '2015'
                    WHEN cohort = 2016 THEN '2016'
                    END as cohort
                    ,region
                    ,week_end as ds
                    ,SUM(spread_ttl) as spread_ttl
                    FROM(
                    SELECT
                    cpm.employee_id as am_id
                    ,r.`name` as region
                    ,YEAR(c.start_date) as cohort
                    ,CASE WHEN cpm.usrtype_id = 128 THEN 'Direct Placement' ELSE 'Contract' END as dp_ind
                    ,cpm.week_start
                    ,date_add(cpm.week_start, INTERVAL 7 - DAYOFWEEK(cpm.week_start) DAY) as week_end
                    ,SUM(cpm.spread)+SUM(cpm.spread_over) as spread_ttl
                    FROM commission_profile_map cpm
                    JOIN (SELECT id,MIN(start_date) start_date FROM employee WHERE start_date <= DATE(DATE_SUB(DATE_FORMAT(NOW(), '%Y-12-31'),INTERVAL 1 YEAR)) GROUP BY id) c ON c.id = cpm.employee_id
                    JOIN office o ON o.id = cpm.office_id
                    JOIN region r ON r.id = o.region_id
                    WHERE
                    usrtype_id IN (3,168,128)
                    AND week_start BETWEEN DATE(DATE_SUB(NOW(),INTERVAL 3 YEAR)) AND DATE_SUB(DATE_ADD(NOW(), INTERVAL 7 - DAYOFWEEK(NOW()) DAY), INTERVAL 7 DAY)
                    GROUP BY
                    cpm.employee_id
                    ,r.`name`
                    ,CASE WHEN cpm.usrtype_id = 128 THEN 'Direct Placement' ELSE 'Contract' END
                    ,cpm.week_start
                    ,YEAR(c.start_date)
                    )off
                    GROUP BY
                    CASE 
                    WHEN cohort <= 2013 THEN '2013'
                    WHEN cohort = 2014 THEN '2014'
                    WHEN cohort = 2015 THEN '2015'
                    WHEN cohort = 2016 THEN '2016'
                    END
                    ,region
                    ,week_end
                    ORDER BY
                    2,1")
co_spread_all_df <- fetch(db.co, n=-1)
#for (i in 1:length(cohort.dt)){
#  assign(paste0("co.df", i), co_spread_all_df[co_spread_all_df$cohort==cohort.dt[i],])
#  assign(paste0("co.df.log", i), co.df[i] %>% mutate(y = log(spread_ttl)))
#}

#co.list <- list()
for (i in 1:length(cohort.dt)){
  if (i != 4){
  co.df <- co_spread_all_df[co_spread_all_df$cohort==cohort.dt[i],]
  co.df.log <- co.df %>% mutate(y = log(spread_ttl))
  #co.list[[i]] <- co.df.log
  m <- prophet(co.df.log)
  future <- make_future_dataframe(m, periods = 12, freq ='w')
  forecast <- predict(m, future)
  co_forecast <- forecast[c('ds', 'yhat')]
  co_forecast$yhat <- exp(forecast$yhat)
  co_forecast <- data.frame(co_forecast$ds, cohort.dt[i], co_forecast$yhat)
  colnames(co_forecast) <- c("ds", "cohort", "f_spr_ttl")
  uat02comdb = dbConnect(MySQL(), user='tganka', password='tganka_123!', dbname='comtrak', host='172.16.125.33')
  dbWriteTable(uat02comdb, "forecast_co_spr_tbl", value=co_forecast, row.name=FALSE, overwrite=FALSE, append=TRUE)
  dbDisconnect(uat02comdb) 
  }else
    if (i == 4){
      co.df <- co_spread_all_df[co_spread_all_df$cohort==cohort.dt[i],]
      co.df.log <- co.df %>% mutate(y = log(spread_ttl))
      outliers <- (as.Date(df$ds) < as.Date('2016-07-16'))
      co.df.log$y[outliers] = NA
      m <- prophet(co.df.log)
      future <- make_future_dataframe(m, periods = 12, freq ='w')
      forecast <- predict(m, future)
      co_forecast <- forecast[c('ds', 'yhat')]
      co_forecast$yhat <- exp(forecast$yhat)
      co_forecast <- data.frame(co_forecast$ds, cohort.dt[i], co_forecast$yhat)
      colnames(co_forecast) <- c("ds", "cohort", "f_spr_ttl")
      uat02comdb = dbConnect(MySQL(), user='tganka', password='tganka_123!', dbname='comtrak', host='172.16.125.33')
      dbWriteTable(uat02comdb, "forecast_co_spr_tbl", value=co_forecast, row.name=FALSE, overwrite=FALSE, append=TRUE)
      dbDisconnect(uat02comdb) 
    }
}
