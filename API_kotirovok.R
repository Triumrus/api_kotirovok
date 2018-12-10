library(RCurl)
library(jsonlite)
library(data.table)
library(RODBC)
uat_conn = odbcConnect("UAT")
seccod<- sqlQuery(uat_conn,"
                  select distinct(SECCODE) from [CRM_data].[dbo].[BO_Limits_NoFilters]
                  where 1=1 
                  and not SECNAME='RUR'
                  and not SECNAME='USD'
                  and CONVERT(Date, DATE, 104) between  CONVERT (date, GETDATE()-180) and CONVERT (date, GETDATE())         
                  ")
# seccod <- "SBER"
seccod<- seccod$SECCODE
seccod<- as.character(seccod)
seccod<-c("USD000UTSTOM","EUR_RUB__TOM",seccod)



from <-  as.numeric(as.POSIXct(Sys.Date()-2000, format="%Y-%m-%d"))
to <-  as.numeric(as.POSIXct(Sys.Date()-1, format="%Y-%m-%d"))
# to <- from

for (z in seccod){
  if (z == "") {next}
  print(z)
  print(which(seccod %in% z)/length(seccod))
  
  url <- URLencode(paste0("http://tv.dev.alor.ru/md/history?format=TV&code=",z,"&exchange=GAME&tf=D&from=",from,"&to=",to))
  # a <- fromJSON(getURL(url))
  a<-   lapply(paste(readLines(url, warn=FALSE),
                     collapse=""), 
               jsonlite::fromJSON)
  
  # a<- a$history
  a<- a[[1]]$history
  # try(print(a[1,]),T)
  a$date <- as.character(format(as.POSIXct(a$time/1000,tz="GMT",origin="1970-01-01"),format="%Y-%m-%d"))
  a$upload_date <- format(Sys.time(),tz="UTC",format="%Y-%m-%d")
  a$time <-  as.character(a$time/1000)
  a$seccode <- z
  try(
    for(i in 1:nrow(a)){
      req <- paste("INSERT INTO [analytics].[dbo].[securities_prices] (time,[close],[open],high,low,volume,date,upload_date,seccode
  ) VALUES
               (",a[i,1],",",a[i,2],",",a[i,3],",",a[i,4],",",a[i,5],","
                   ,a[i,6],",'",a[i,7],"','",a[i,8],"','",a[i,9],"')"
                   ,sep = "")
      
      sqlQuery(uat_conn,req)
      # print(i/nrow(a))
      
    }
    ,T)
}

odbcClose(uat_conn)





