##server.R


#libraries 
library(ggplot2)
library(reshape2)
library(shiny)
library(DT)
library(formattable)
library(scales)
library(eeptools)

function(input, output) {


#load date 
load("dfstoriadup.RData")
load("Storiadupfinal2.RData")
load("dfstradia.RData")
load("Stradiadupfinal3.Rdata")

#Ajust similarity percentage (Shiny has some problems with that)

Storiadupfinal2$similarity <- paste(round(Storiadupfinal2$similarity*100,digits=1),"%",sep="")

Stradiadupfinal3$similarity <- paste(round(Stradiadupfinal3$similarity*100,digits=1),"%",sep="")

#Tables(daily duplicates)

output$ex1 <- DT::renderDataTable(
DT::datatable(Storiadupfinal2, options = list(pageLength = 25))
  )
  
output$ex2 <- DT::renderDataTable(
  DT::datatable(Stradiadupfinal3, options = list(pageLength = 25))
)

                        
#plot (evolution by month)

#Storia Graph 
output$duplicatesPlot <- renderPlot({
  
  ggplot(df, aes(Date)) + 
    geom_bar(width=.5,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
    geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
    scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
    coord_cartesian(ylim = c(70000, 320000)) + 
    geom_text(aes(y= Duplicates,label = Duplicates, vjust=-2)) +
    geom_text(aes(y= Ads,label = Ads, vjust=2))
              

})

#Stradia Graph
output$duplicatesPlot2 <- renderPlot({

ggplot(dfstradia, aes(Date)) + 
  geom_bar(width=.8,aes(y = Ads, color = "Ads"), stat="identity", fill = "orange") +
  geom_line(aes(y = Duplicates, group = 1, color = "Duplicates")) +
  scale_colour_manual("", values=c("Duplicates" = "blue", "Ads" = "orange")) + 
  coord_cartesian(ylim = c(0, 55000)) + 
  geom_text(aes(y= Duplicates,label = Duplicates, vjust=-1)) + 
  geom_text(aes(y= Ads,label = Ads, vjust=2))

})

}





































