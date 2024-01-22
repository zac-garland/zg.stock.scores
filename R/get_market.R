get_market_data <- function() {
  if (attr(sp500, "last_updated") < Sys.Date()) {
    message("updating sp500")
    sp500 <- fmpr::fmp_historical_price("^GSPC", start_date = (Sys.Date() %m-% lubridate::years(100)))
    attr(sp500, "last_updated") <- Sys.Date()
    usethis::use_data(sp500, overwrite = TRUE)
    load(here::here("data/sp500.rda"))
  }

  sp500
}
