
get_qrs_data <- function(ticker, return_sql_connect = FALSE, debug = FALSE) {
  mod_full_join <- function(lhs, rhs, ...) {
    if (nrow(rhs) > 0) {
      dplyr::full_join(lhs, rhs, ...)
    } else {
      lhs
    }
  }

  ticker_price <- fmpr::fmp_historical_price(ticker = ticker, start_date = lubridate::as_date("1900-01-01")) %>%
    dplyr::mutate(symbol = ticker)
  market_data <- get_market_data() %>% dplyr::select(date, market_close = adj_close)
  analyst_est <- fmpr::fmp_analyst_estimates(ticker, "annual") %>% dplyr::mutate(symbol = ticker)
  key_metrics <- fmpr::fmp_key_metrics(ticker = ticker, period = "quarter") %>% dplyr::mutate(symbol = ticker)
  earnings_cal <- fmpr::fmp_historical_earning_calendar(ticker = ticker) %>% dplyr::mutate(symbol = ticker)
  income_statement <- fmpr::fmp_income_statement(ticker = ticker) %>% dplyr::mutate(symbol = ticker)
  cash_statement <- fmpr::fmp_cash_flow_statement(ticker = ticker) %>% dplyr::mutate(symbol = ticker)
  balance_sheet <- fmpr::fmp_balance_sheet(ticker = ticker) %>% dplyr::mutate(symbol = ticker)
  q_ratios <- fmpr::fmp_ratios(ticker = ticker) %>% dplyr::mutate(symbol = ticker)

  safe_get_employ <- purrr::safely(fmp_historical_employment, otherwise = dplyr::tibble())

  employment <- safe_get_employ(ticker = ticker)$result
  if (nrow(employment) > 0) {
    employment <- employment %>%
      dplyr::mutate(symbol = ticker) %>%
      dplyr::select(symbol, date = filing_date, employee_count)
  }


  # market factors ----------------------------------------------------------

  # up down beta
  ticker_up_down_beta <- ticker_price %>%
    dplyr::arrange(date) %>%
    dplyr::select(symbol, date, adj_close) %>%
    dplyr::left_join(market_data) %>%
    dplyr::mutate_at(dplyr::vars(adj_close, market_close), dplyr::funs(. / dplyr::lag(.) - 1)) %>%
    tidyquant::tq_mutate(
      mutate_fun = rollapply,
      width = 255,
      FUN = function(data) {
        data %>%
          timetk::tk_tbl() %>%
          dplyr::summarize(
            up_beta = cor(adj_close[market_close >= 0], market_close[market_close >= 0], use = "pairwise.complete.obs"),
            down_beta = cor(adj_close[market_close <= 0], market_close[market_close <= 0], use = "pairwise.complete.obs"),
            up_minus_down_beta = up_beta - down_beta
          ) %>%
          tidyr::gather(key, value) %>%
          {
            setNames(.$value, .$key)
          }
      },
      by.column = FALSE
    )

  # lt / st rsi, ulcer index, lt / st momentum, ulcer index, 1year volatility
  ticker_mkt_metrics <- ticker_price %>%
    dplyr::arrange(date) %>%
    tidyquant::tq_mutate(
      select = adj_close,
      mutate_fun = RSI,
      col_rename = "RSI_ST",
      n = 12 * 28
    ) %>%
    tidyquant::tq_mutate(
      select = adj_close,
      mutate_fun = RSI,
      col_rename = "RSI_LT",
      n = 3 * 28
    ) %>%
    dplyr::mutate(
      lt_st_rsi = RSI_LT - RSI_ST
    ) %>%
    dplyr::mutate(
      one_year_vol = roll::roll_sd(adj_close, 28 * 12)
    ) %>%
    dplyr::mutate(
      lt_st_mom = (adj_close / dplyr::lag(adj_close, 28 * 12) - 1) - (adj_close / dplyr::lag(adj_close, 28) - 1)
    ) %>%
    dplyr::mutate(
      ulcer_index = ((adj_close - roll::roll_max(adj_close, 300)) / roll::roll_max(adj_close, 300)) * 100,
      ulcer_index = abs(((ulcer_index) / 300)),
      ulcer_index = sqrt(ulcer_index)
    )



  # fundamental factors -----------------------------------------------------

  if (nrow(analyst_est) > 0) {
    analyst_estimates <- analyst_est %>%
      dplyr::select(symbol, date, estimated_eps_avg, estimated_revenue_avg) %>%
      dplyr::mutate(
        fy1_eps = dplyr::lead(estimated_eps_avg, 1),
        fy2_eps = dplyr::lead(estimated_eps_avg, 2)
      ) %>%
      dplyr::mutate(
        long_term_growth = roll::roll_mean(estimated_eps_avg / dplyr::lag(estimated_eps_avg, 1) - 1, 5),
        long_term_sales_growth = roll::roll_mean(estimated_revenue_avg / dplyr::lag(estimated_revenue_avg, 1) - 1, 5)
      ) %>%
      dplyr::select(symbol, date, fy1_eps, fy2_eps, long_term_growth, long_term_sales_growth)
  } else {
    analyst_estimates <- analyst_est
  }




  ttm_earnings <- earnings_cal %>%
    dplyr::select(symbol, date, eps) %>%
    dplyr::mutate(eps = roll::roll_sum(eps, 4))


  key_measures <- key_metrics %>%
    dplyr::select(symbol, date,
      book_value_per_share,
      tangible_book_value_per_share,
      operating_cash_flow_per_share,
      free_cash_flow_per_share,
      revenue_per_share,
      roe,
      roic,
      roa = return_on_tangible_assets,
      current_ratio,
      dividend_yield,
      debt_to_equity,
      receivables_turnover,
      net_income_per_share
    ) %>%
    dplyr::mutate(
      cash_flow_var = roll::roll_median(
        operating_cash_flow_per_share / net_income_per_share, 12
      ),
      net_income_var = roll::roll_sd(
        net_income_per_share / revenue_per_share, 12
      )
    )







  cash_measures <- cash_statement %>%
    dplyr::select(date, capital_expenditure, operating_cash_flow,
      cash_from_invest = net_cash_used_for_investing_activites,
      free_cash_flow
    ) %>%
    dplyr::left_join(
      balance_sheet %>%
        dplyr::mutate(
          invested_capital = short_term_debt + long_term_debt + capital_lease_obligations + total_equity + cash_and_short_term_investments
        ) %>%
        dplyr::select(date, symbol, total_assets, total_current_assets, total_current_liabilities, total_stockholders_equity, total_equity, total_debt, net_debt, invested_capital) %>%
        dplyr::mutate(total_capital = total_current_assets - total_current_liabilities)
    ) %>%
    dplyr::mutate(
      cash_flow_coverage = operating_cash_flow / total_debt,
      capex_to_assets = capital_expenditure / total_assets,
      debt_to_capital = total_debt / total_capital,
      financial_leverage = total_debt / total_stockholders_equity,
      cfi_to_invest_cap = (-1 * cash_from_invest) / invested_capital
    )

  ratio_measures <- q_ratios %>%
    dplyr::select(symbol, date, quick_ratio, asset_turnover, gross_profit_margin, operating_profit_margin, net_profit_margin)

  qrs_out <- ticker_mkt_metrics %>%
    dplyr::select(symbol, date, adj_close, lt_st_rsi, one_year_vol, lt_st_mom, ulcer_index) %>%
    dplyr::left_join(
      ticker_up_down_beta %>%
        dplyr::select(symbol, date, up_minus_down_beta)
    ) %>%
    mod_full_join(
      analyst_estimates
    ) %>%
    mod_full_join(
      ttm_earnings
    ) %>%
    mod_full_join(
      key_measures
    ) %>%
    mod_full_join(
      cash_measures
    ) %>%
    mod_full_join(
      ratio_measures
    ) %>%
    mod_full_join(
      income_statement %>%
        dplyr::select(symbol, date, revenue)
    ) %>%
    mod_full_join(
      employment
    ) %>%
    dplyr::arrange(date) %>%
    tidyr::fill(where(function(x) is.numeric(x)) & !matches("adj_close")) %>%
    dplyr::filter(!is.na(adj_close)) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c(
          "fy1_eps", "fy2_eps", "eps", "book_value_per_share", "tangible_book_value_per_share",
          "operating_cash_flow_per_share", "free_cash_flow_per_share", "revenue_per_share"
        )),
        list(yield = ~ . / adj_close)
      )
    ) %>%
    dplyr::mutate(equity_turnover = revenue / total_stockholders_equity) %>%
    dplyr::mutate(cf_per_employee = free_cash_flow / employee_count)

  # qrs_out

  cols_out <- c(
    "date", "symbol", "one_year_vol", "lt_st_mom", "lt_st_rsi",
    "ulcer_index", "up_minus_down_beta", "eps_yield", "fy1_eps_yield",
    "fy2_eps_yield", "dividend_yield", "book_value_per_share_yield",
    "tangible_book_value_per_share_yield", "operating_cash_flow_per_share_yield",
    "free_cash_flow_per_share_yield", "revenue_per_share_yield",
    "roe", "roa", "roic", "gross_profit_margin", "operating_profit_margin",
    "net_profit_margin", "current_ratio", "quick_ratio", "cash_flow_coverage",
    "capex_to_assets", "debt_to_capital", "debt_to_equity", "financial_leverage",
    "cfi_to_invest_cap", "long_term_growth", "long_term_sales_growth",
    "net_income_var", "cash_flow_var", "asset_turnover", "equity_turnover",
    "receivables_turnover", "cf_per_employee"
  )

  for (i in seq_along(cols_out)) {
    if (!cols_out[i] %in% names(qrs_out)) {
      qrs_out[cols_out[i]] <- NA
    }
  }

  qrs_out %>%
    dplyr::select(
      date, symbol, adj_close,
      # market_intercept,
      # market_slope,
      # market_residual_momentum,
      # market_residual_volatility,
      # market_residual_ratio,
      market_1_y_volatility = one_year_vol,
      market_lt_st_momentum = lt_st_mom,
      market_lt_st_rsi = lt_st_rsi,
      market_lt_ulcer_index = ulcer_index,
      market_up_down_beta = up_minus_down_beta,
      value_earnings_yield = eps_yield,
      value_earnings_yield_fy_1 = fy1_eps_yield,
      value_earnings_yield_fy_2 = fy2_eps_yield,
      value_dividend_yield = dividend_yield,
      value_book_price = book_value_per_share_yield,
      value_tangible_book_price = tangible_book_value_per_share_yield,
      value_operating_cf_yield = operating_cash_flow_per_share_yield,
      value_free_cash_flow_yield = free_cash_flow_per_share_yield,
      value_sales_price = revenue_per_share_yield,
      profitability_roe = roe,
      profitability_roa = roa,
      profitability_roic = roic,
      profitability_gross_margin = gross_profit_margin,
      profitability_operating_margin = operating_profit_margin,
      profitability_net_margin = net_profit_margin,
      solvency_current_ratio = current_ratio,
      solvency_quick_ratio = quick_ratio,
      solvency_cash_flow_coverage = cash_flow_coverage,
      solvency_capex_assets = capex_to_assets,
      solvency_debt_capital = debt_to_capital,
      solvency_debt_equity = debt_to_equity,
      solvency_financial_leverage = financial_leverage,
      # growth_market_share_e_growth = lon,
      growth_cfi_invested_capital = cfi_to_invest_cap,
      growth_ltg = long_term_growth,
      # growth_dividend_growth_g,
      growth_predicted_sales_growth = long_term_sales_growth,
      # growth_high_roic_eps_growth,
      # quality_piotroski_score,
      quality_net_profit_variability = net_income_var,
      quality_cash_flow_variability = cash_flow_var,
      # quality_holt_accounting_quality,
      # sentiment_estimate_revisions,
      # sentiment_news_sentiment,
      # sentiment_share_repurchases,
      # sentiment_short_interest,
      efficiency_asset_turnover = asset_turnover,
      efficiency_equity_turnover = equity_turnover,
      efficiency_receivables_turnover = receivables_turnover,
      efficiency_cash_flow_employee = cf_per_employee
    ) %>%
    tibble::add_column(
      retrieved_date = Sys.time(), .before = 1
    ) %>%
    tibble::add_column(
      retrieved_sys_date = Sys.Date(), .before = 1
    ) %>%
    dplyr::mutate(month_date = lubridate::ceiling_date(date, "month")) %>%
    dplyr::select(retrieved_sys_date, retrieved_date, month_date, date, symbol, dplyr::everything()) %>%
    dplyr::group_by(month_date) %>%
    dplyr::filter(date == max(date, na.rm = TRUE)) %>%
    dplyr::select(-date) %>%
    dplyr::rename(date = month_date) %>%
    dplyr::ungroup() -> out_res

  if (debug) {
    return(out_res)
  }

  if (nrow(out_res) > 0) {
    mydb <- connect_quant()
    on.exit(DBI::dbDisconnect(mydb), add = TRUE)
    out_res <- out_res %>%
      dplyr::mutate_at(dplyr::vars(dplyr::contains("date")), dplyr::funs(as.character)) %>%
      dplyr::mutate(retrieved_date = anytime::anytime(retrieved_date) %>% as.double())
    if (!("qrs_data" %in% DBI::dbListTables(mydb))) {
      DBI::dbWriteTable(mydb, "qrs_data", out_res)
    } else {
      DBI::dbExecute(mydb, as.character(glue::glue("DELETE FROM qrs_data WHERE (symbol = '{ticker}')")))

      DBI::dbAppendTable(mydb, "qrs_data", out_res)
    }
  }

  if (return_sql_connect) {
    dplyr::tbl(mydb, "qrs_data")
  } else {
    DBI::dbDisconnect(mydb)
  }
}


fill_qrs_table <- function(test = FALSE, quiet = TRUE) {
  library(furrr)
  sp50 <- fmpr::fmp_sp500_constituents()
  # filt_ticks <- quant_table() %>%
  #   distinct(retrieved_sys_date, symbol) %>%
  #   dplyr::filter(retrieved_sys_date %in% local(as.character(seq.Date(Sys.Date()-5,Sys.Date(),by = "day")))) %>%
  #   collect() %>%
  #   dplyr::pull(symbol)


  if (nrow(sp50) > 0) {
    mydb <- connect_quant()
    on.exit(DBI::dbDisconnect(mydb),add = TRUE)
    sp50 <- sp50 %>%
      tibble::add_column(
        retrieved_date = Sys.time(), .before = 1
      ) %>%
      tibble::add_column(
        retrieved_sys_date = Sys.Date(), .before = 1
      ) %>%
      dplyr::mutate_at(dplyr::vars(dplyr::contains("date")), dplyr::funs(as.character)) %>%
      dplyr::mutate(retrieved_date = anytime::anytime(retrieved_date) %>% as.double())


    if (!("quant_qrs_meta" %in% DBI::dbListTables(mydb))) {
      DBI::dbWriteTable(mydb, "quant_qrs_meta", sp50)
    } else {
      DBI::dbAppendTable(mydb, "quant_qrs_meta", sp50)
    }


    sp50 %>%
      # dplyr::arrange(sector,desc(weight)) %>%
      # dplyr::group_by(sector) %>%
      # dplyr::filter(row_number() %in% 1:25) %>%
      # dplyr::filter(!symbol %in% filt_ticks) %>%
      # dplyr::ungroup() %>%
      dplyr::pull(symbol) -> sp50_ticks

    if (test) {
      sp50_ticks <- sample(sp50_ticks, 3)
    }


    future::plan(multisession)



    furrr::future_map(sp50_ticks, ~ {
      get_qrs_data_safe <- purrr::safely(get_qrs_data)
      # message(glue::glue("retrieving {.x}"))
      # get_qrs_data(.x)
      if (quiet) {
        suppressMessages(get_qrs_data_safe(.x)) %>% invisible()
      } else {
        get_qrs_data_safe(.x)
      }
    })
  }
}
