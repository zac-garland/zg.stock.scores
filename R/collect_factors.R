get_complete_factor_list <- function() {
  qrs_out <- dplyr::tbl(connect_quant(), "qrs_data") %>%
    dplyr::group_by(symbol) %>%
    dplyr::filter(retrieved_date == max(retrieved_date)) %>%
    dplyr::select(-contains("retrieved")) %>%
    dplyr::collect() %>%
    dplyr::mutate(date = lubridate::as_date(date)) %>%
    dplyr::arrange(symbol, date) %>%
    dplyr::group_by(symbol) %>%
    dplyr::mutate(one_month_fwd = dplyr::lead(adj_close, 1) / adj_close - 1) %>%
    dplyr::select(date, symbol, adj_close, one_month_fwd, dplyr::everything()) %>%
    tidyr::gather(key, value, -any_of(c("date", "symbol", "adj_close", "one_month_fwd"))) %>%
    dplyr::filter(!is.na(value))

  date_filt <- qrs_out %>%
    dplyr::filter(!is.na(one_month_fwd)) %>%
    dplyr::ungroup() %>%
    dplyr::distinct(date, symbol) %>%
    dplyr::count(date) %>%
    dplyr::filter(n >= dplyr::lag(n)) %>%
    dplyr::filter(date == max(date, na.rm = TRUE)) %>%
    dplyr::pull(date)


  key_join <- qrs_out %>%
    dplyr::ungroup() %>%
    dplyr::distinct(key) %>%
    dplyr::mutate(key_copy = key) %>%
    tidyr::separate(key_copy, c("group", "factor"), sep = "_", extra = "merge") %>%
    dplyr::mutate_at(dplyr::vars(group, factor), dplyr::funs(snakecase::to_title_case))


  meta_info <- dplyr::tbl(connect_quant(), "quant_qrs_meta") %>%
    dplyr::group_by(symbol) %>%
    dplyr::filter(retrieved_date == max(retrieved_date)) %>%
    dplyr::collect() %>%
    dplyr::distinct(symbol, name, sector, sub_sector)





  normal_val <- qrs_out %>%
    dplyr::left_join(meta_info) %>%
    dplyr::left_join(key_join) %>%
    dplyr::select(where(is.character), dplyr::everything()) %>%
    dplyr::group_by(date, sector, group, factor) %>%
    dplyr::mutate(
      normalized_value = dplyr::ntile(value, n = 5),
      z_score = (value - mean(value, na.rm = TRUE)) / sd(value, na.rm = TRUE)
    ) %>%
    dplyr::filter(!is.na(one_month_fwd), !is.na(value)) %>%
    dplyr::filter(date <= date_filt)


  group_factor_weights <- normal_val %>%
    dplyr::arrange(sector, group, factor, date) %>%
    dplyr::group_by(date, sector, group, factor) %>%
    tidyr::nest() %>%
    dplyr::mutate(ic = purrr::map_dbl(data, ~ {
      cor(.$one_month_fwd, .$z_score, use = "pairwise.complete.obs")
    })) %>%
    dplyr::select(-data) %>%
    dplyr::group_by(sector, group, factor) %>%
    dplyr::mutate(trailing_ic = roll::roll_mean(ic, 12)) %>%
    dplyr::group_by(sector, group, date) %>%
    dplyr::mutate(weight = abs(trailing_ic) / sum(abs(trailing_ic), na.rm = TRUE)) %>%
    dplyr::filter(!is.na(weight)) %>%
    dplyr::mutate(lvrb = trailing_ic / abs(trailing_ic)) %>%
    dplyr::select(sector, group, factor, date, weight, lvrb)

  # group_factor_weights


  mfr_scores <- normal_val %>%
    dplyr::left_join(group_factor_weights) %>%
    dplyr::mutate(z_score = min_max(lvrb * z_score)) %>%
    dplyr::mutate(contr = z_score * weight) %>%
    dplyr::group_by(symbol, name, sector, sub_sector, group, date) %>%
    dplyr::summarize(z_score = sum(contr, na.rm = TRUE)) %>%
    dplyr::mutate(factor = group)


  alpha_weights <- mfr_scores %>%
    dplyr::left_join(
      normal_val %>%
        dplyr::ungroup() %>%
        dplyr::select(symbol, date, one_month_fwd) %>%
        dplyr::distinct()
    ) %>%
    dplyr::arrange(sector, group, factor, date) %>%
    dplyr::group_by(date, sector, group, factor) %>%
    tidyr::nest() %>%
    dplyr::mutate(ic = purrr::map_dbl(data, ~ {
      cor(.$one_month_fwd, .$z_score, use = "pairwise.complete.obs")
    })) %>%
    dplyr::select(-data) %>%
    dplyr::group_by(sector, group, factor) %>%
    dplyr::mutate(trailing_ic = roll::roll_mean(ic, 12)) %>%
    dplyr::group_by(sector, date) %>%
    dplyr::mutate(weight = abs(trailing_ic) / sum(abs(trailing_ic), na.rm = TRUE)) %>%
    dplyr::filter(!is.na(weight)) %>%
    dplyr::mutate(lvrb = trailing_ic / abs(trailing_ic)) %>%
    dplyr::select(sector, group, factor, date, weight, lvrb)


  alpha_scores <- mfr_scores %>%
    dplyr::left_join(alpha_weights) %>%
    dplyr::mutate(z_score = min_max(lvrb * z_score)) %>%
    dplyr::mutate(contr = z_score * weight) %>%
    dplyr::group_by(symbol, name, sector, sub_sector, date) %>%
    dplyr::summarize(z_score = sum(contr, na.rm = TRUE)) %>%
    dplyr::mutate(factor = "alpha", group = "alpha")



  normal_val %>%
    dplyr::ungroup() %>%
    dplyr::distinct(symbol, date, factor, group, z_score) %>%
    dplyr::full_join(
      mfr_scores %>% dplyr::ungroup() %>% dplyr::distinct(symbol, date, factor, group, z_score)
    ) %>%
    dplyr::full_join(
      alpha_scores %>% dplyr::ungroup() %>% dplyr::distinct(symbol, date, factor, group, z_score)
    ) %>%
    dplyr::left_join(
      normal_val %>% dplyr::ungroup() %>% dplyr::distinct(symbol, name, date, sector, sub_sector, one_month_fwd)
    )
}


get_factor_ic <- function(complete_factor_list) {
  complete_factor_list %>%
    dplyr::group_by(group, factor) %>%
    dplyr::summarize(cor = cor(z_score, one_month_fwd, use = "pairwise.complete.obs")) %>%
    dplyr::arrange(dplyr::desc(abs(cor))) %>%
    print(n = nrow(.))
}


get_fn_f1_perf <- function(complete_factor_list) {
  complete_factor_list %>%
    dplyr::group_by(sector, group, date, factor) %>%
    dplyr::mutate(normalized_value = dplyr::ntile(z_score, 5)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(group, factor, normalized_value) %>%
    dplyr::summarize(return = ((1 + mean(one_month_fwd, na.rm = TRUE))^12) - 1) %>%
    tidyr::spread(normalized_value, return) %>%
    dplyr::mutate(fn_f1 = `5` - `1`) %>%
    dplyr::arrange(dplyr::desc(abs(fn_f1))) %>%
    print(n = nrow(.))
}


get_invest_list <- function(complete_factor_list) {
  complete_factor_list %>%
    dplyr::filter(factor == "alpha") %>%
    dplyr::group_by(sector, group, factor, date) %>%
    dplyr::mutate(normalized_value = dplyr::ntile(z_score, 5)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(date == max(date, na.rm = TRUE)) %>%
    dplyr::filter(normalized_value == 5) %>%
    dplyr::select(symbol, name, sector, sub_sector, z_score, one_month_fwd) %>%
    dplyr::mutate(one_month_fwd = scales::percent(one_month_fwd, accuracy = 0.01))
}

plot_alpha_historical_perf <- function(complete_factor_list, from_date = "2020-01-01") {
  complete_factor_list %>%
    dplyr::filter(factor == "alpha") %>%
    dplyr::group_by(sector, group, factor, date) %>%
    dplyr::mutate(normalized_value = dplyr::ntile(z_score, 5)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(group, factor, date, normalized_value) %>%
    dplyr::summarize(one_month_fwd = mean(one_month_fwd, na.rm = TRUE)) %>%
    dplyr::filter(date >= lubridate::as_date(from_date)) %>%
    dplyr::arrange(group, factor, normalized_value, date) %>%
    dplyr::group_by(group, factor, normalized_value) %>%
    dplyr::mutate(cum_ret = dplyr::with_order(date, cumprod, 1 + one_month_fwd) - 1) %>%
    dplyr::group_by(group, factor) %>%
    dplyr::group_split() %>%
    purrr::map(~ {
      highcharter::hchart(.x, "line", highcharter::hcaes(date, cum_ret * 100, group = normalized_value)) %>%
        highcharter::hc_title(text = glue::glue("zg {unique(.x$factor)}")) %>%
        highcharter::hc_add_series(get_market_data() %>% dplyr::mutate(
          one_month_fwd = (dplyr::lead(adj_close, 20) / adj_close - 1) / 20
        ) %>%
          dplyr::filter(date >= lubridate::as_date(from_date)) %>%
          dplyr::mutate(cum_ret = dplyr::with_order(date, cumprod, 1 + one_month_fwd) - 1),
        "line", highcharter::hcaes(date, cum_ret * 100),
        name = "S&P 500"
        ) %>%
        highcharter::hc_yAxis(title = list(text = ""), labels = list(format = "{value}%"))
    }) %>%
    highcharter::hw_grid(rowheight = 800)
}
