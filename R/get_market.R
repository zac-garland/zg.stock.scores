get_market_data <- function() {

  mydb <- connect_quant()
  on.exit(DBI::dbDisconnect(mydb),add=TRUE)

  if (!("sp500" %in% DBI::dbListTables(mydb))) {
    sp500 <- fmpr::fmp_historical_price("^GSPC", start_date = (Sys.Date() %m-% lubridate::years(100))) %>%
      dplyr::mutate(collected_date = Sys.Date(),date = as.character(date))

    DBI::dbWriteTable(mydb, "sp500", sp500)
  } else if((dplyr::tbl(connect_quant(),"sp500") %>% dplyr::distinct(collected_date) %>% dplyr::pull()) < Sys.Date()){

    sp500 <- fmpr::fmp_historical_price("^GSPC", start_date = (Sys.Date() %m-% lubridate::years(100))) %>%
      dplyr::mutate(collected_date = Sys.Date(),date = as.character(date))

    DBI::dbWriteTable(mydb, "sp500", sp500,overwrite = TRUE)
  }else{
   sp500 <- dplyr::tbl(connect_quant(),"sp500") %>%
      dplyr::collect() %>%
      dplyr::select(-collected_date)
  }


  sp500 %>%
    dplyr::mutate(date = as.Date(date))
}
