setwd('/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/Data/cleaned')

the = c("test","R","code","is","working")
write(the, "/media/mint/e834712c-23da-4cbe-a4ec-3a35d416877b/Run_system/test.txt")

plot2<-function(dataset,scraper=0){
  
  print(dataset$product_name)
  subset<- subset(dataset,
                  select = c(product_name, ons_item_no,
                             monthday))
  
  prodlist <- unique(subset$product_name)
  #changing the datelist so days when web scraper failed turn up
  if (scraper==0){
    datelist <- unique(subset$monthday)                             
  } else if (scraper == 1){
    current_date = Sys.Date()
    current_date = as.Date(current_date,format='%m%d%Y') #create list of dates 7 days before current date
    back_date = rev(current_date - 0:7)
	datelist = back_date
	 
  }                           
  
  a <- matrix(0, length(prodlist)*length(datelist),4)
  for (d in c(1:length(datelist))){
    temp <- as.vector(subset[subset$monthday == datelist[d], 1])
    #print(datelist[d])
    for (p in c(1:length(prodlist))){
      row <- (d-1) * length(prodlist) + p 
      a[row,1] <- p
      a[row,2] <- d * pmatch(prodlist[p],temp)/pmatch(prodlist[p],temp)
      a[row,3]<-datelist[d]
      a[row,4]<-as.character(prodlist[p])
    }
  }
  b<-data.frame( prodno=a[,1],yy=a[,2],date=a[,3],prodname=a[,4])
  b$startdate<-0
  b$enddate<-0
  
  datecheck<-function(x){
    x1<-subset(x,is.na(x$yy)==F)
    x$startdate<-min(as.numeric(as.character(x1$date)))
    x$enddate<-max(as.numeric(as.character(x1$date)))
    return(x)
  }
  b1<-ddply(b,.(prodno), function(b) datecheck(b))
  b2<-b1[order(b1$startdate,b1$enddate),]
  prodlist2<-unique(b2$prodname)
  b2$pn2<--10
  b3<-b2[1,]
  for (p in 1:length(prodlist2)){
    y<-subset(b2,b2$prodname==prodlist2[p])
    y$pn2<-p
    b3<-rbind(b3,y)
  }
  b3<-b3[b3$pn2>0,]
  
  b3$yy<-as.numeric(as.character(b3$yy))
  return(b3)
  #print(ggplot(b3,aes(yy,pn2))+geom_tile()+xlab("Day Number")+ylab("Product Number"))
  #if (list==1){
  #  print(prodlist2)
  #} 
}
require(plyr)





allimp<-function(x){
  print("Starting Preparation")
  missingdays<-ddply(x,.(product_name),function(x) plot2(x,1))
  names(missingdays)[4]<-c("monthday")
  b1<-merge(x,missingdays,by=c("product_name","monthday"),all=T)
  print("Preperation Done")
  return(b1)
}


items<-c(210111,210113,210204,210213,210214,210302,211305,211501,211709,211710,211807,
         211814,211901,212006,212011,212016,212017,212319,212360,212515,212519,212717,
         212719,212720,212722,310207,310215,310218,310401,310403,310405,310419,310421,
         310427) 

for (i in items[0:34]){
  print("Item currently being imputed is :-")
  print(i)
  path<-paste('cleaned_chunk',i,'_main_.csv',sep="")
  #path<-paste('missing_aug_',i,'.csv',sep="") 
  test <- read.csv(path)
  #test<-test[test$monthday>=20140601 & test$monthday<20160809,]
  test<-subset(test,select=c("ons_item_no","product_name","store","monthday",
                             "offer","ons_item_name", "std_price",     
                             "timestamp","month","week","unit_price",
                             "ML_score","monthday","trained_cluster2",
                             "check2","QA","item_price_num"))
  ptm <- proc.time()
  df<-allimp(test)
  path2<-paste('missing_aug_',i,'.csv',sep="")
  write.csv(df,path2)
  proc.time() - ptm
  rm(test)
  rm(df)
  rm(path)
  rm(path2)
  gc()
}



