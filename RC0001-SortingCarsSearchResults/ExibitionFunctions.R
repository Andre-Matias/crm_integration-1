#Load the file containing the DataFrame with Autovit data
load("ExibitionAutovit.RData")
#Load the file containing DataFrame with Otomot data
load("ExibitionOtomoto.RData")

TotalExibitionAutovit <- function(input){
  KMSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    3])
  PESorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    4])
  TotalSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    2])
  
  KMSortingPer <- percent(round(sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    3])/
      sum (TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],
        2]),4))
  
  PESortingPer <- percent(round(sum(TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2]
    ,4])/
      sum(TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],2]
        ,2),4))
  
  TotalexibAuto <- data.frame(Total = "Total          .", 
                              "KM Sorting"    = KMSorting,
                              "KM Sorting %" = KMSortingPer,
                              "PE Sorting"    = PESorting,
                              "PE Sorting %" = PESortingPer,
                              "Total Sorting" = TotalSorting
  )
}


TotalExibitionOtomoto <- function(input){
  KMSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    3])
  PESorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    4])
  TotalSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    2])
  
  KMSortingPer <- percent(round(sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    3])/
      sum (TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],
        2]),4))
  
  PESortingPer <- percent(round(sum(TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2]
    ,4])/
      sum(TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],2]
        ,2),4))
  
  TotalexibOto <- data.frame(Total = "Total          .", 
                             "KM Sorting"    = KMSorting,
                             "KM Sorting %a" = KMSortingPer,
                             "PE Sorting"    = PESorting,
                             "PE Sorting %" = PESortingPer,
                             "Total Sorting" = TotalSorting
  )
}