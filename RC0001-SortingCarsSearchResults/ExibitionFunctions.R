#Load the file containing the DataFrame with Autovit data
load("ExibitionAutovit.RData")
#Load the file containing DataFrame with Otomot data
load("ExibitionOtomoto.RData")
 
TotalExibitionAutovit <- function(input){
  PriceSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    4])
  CreateAtSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    3])
  KMSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    5])
  PESorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    6])
  TotalSorting <-  sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    2])
  
  PriceSortingPer <- percent(round(sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    4])/
      sum (TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],
        2]),4))
  
  CreatedAtSortingPer <- percent(round(sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    3])/
      sum (TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],
        2]),4))
  
  KMSortingPer <- percent(round(sum (TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2],
    5])/
      sum (TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],
        2]),4))
  
  PESortingPer <- percent(round(sum(TotalAutovit[
    TotalAutovit$Date >= input$date_range[1] & 
      TotalAutovit$Date <= input$date_range[2]
    ,6])/
      sum(TotalAutovit[
        TotalAutovit$Date >= input$date_range[1] & 
          TotalAutovit$Date <= input$date_range[2],2]
        ,2),4))
  
  TotalexibAuto <- data.frame(Total = "Total          .", 
                              "Price Sorting"    = PriceSorting,
                              "Price Sorting %" = PriceSortingPer,
                              "Created at Sorting"    = CreateAtSorting,
                              "Created at Sorting %" = CreatedAtSortingPer,
                              "KM Sorting"    = KMSorting,
                              "KM Sorting %" = KMSortingPer,
                              "PE Sorting"    = PESorting,
                              "PE Sorting %" = PESortingPer,
                              "Total Sorting" = TotalSorting
  )
}


TotalExibitionOtomoto <- function(input){
  PriceSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    4])
  CreatedAtSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    3])
  KMSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    5])
  PESorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    6])
  TotalSorting <-  sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    2])
  
  PriceSortingPer <- percent(round(sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    4])/
      sum (TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],
        2]),4))
  
  CreateAtSortingPer <- percent(round(sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    3])/
      sum (TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],
        2]),4))
  
  KMSortingPer <- percent(round(sum (TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2],
    5])/
      sum (TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],
        2]),4))
  
  PESortingPer <- percent(round(sum(TotalOtomoto[
    TotalOtomoto$Date >= input$date_range[1] & 
      TotalOtomoto$Date <= input$date_range[2]
    ,6])/
      sum(TotalOtomoto[
        TotalOtomoto$Date >= input$date_range[1] & 
          TotalOtomoto$Date <= input$date_range[2],2]
        ,2),4))
  
  TotalexibOto <- data.frame(Total = "Total          .", 
                             "Price Sorting"    = PriceSorting,
                             "Price Sorting %" = PriceSortingPer,
                             "Created at Sorting"    = CreatedAtSorting,
                             "Created at Sorting %" = CreateAtSortingPer,
                             "KM Sorting"    = KMSorting,
                             "KM Sorting %" = KMSortingPer,
                             "PE Sorting"    = PESorting,
                             "PE Sorting %" = PESortingPer,
                             "Total Sorting" = TotalSorting
  )
}

