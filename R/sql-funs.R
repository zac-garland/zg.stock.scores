connect_quant <- function(){
  dbConnect(RSQLite::SQLite(), "sql/quant-db.sqlite")
}

quant_table <- function(con = connect_quant()){
  dplyr::tbl(con, "quant_data") %>%
    dplyr::group_by(retrieved_sys_date, symbol) %>%
    dplyr::filter(retrieved_date == max(retrieved_date, na.rm = TRUE)) %>%
    dplyr::select(-ends_with(".y"),-calendar_year.x) %>%
    dplyr::rename_at(dplyr::vars(dplyr::ends_with(".x")),funs(stringr::str_remove(.,".x")))
}

quant_meta <- function(con = connect_quant()){
  dplyr::tbl(con, "quant_meta") %>%
    dplyr::group_by(retrieved_sys_date, symbol) %>%
    dplyr::filter(retrieved_date == max(retrieved_date, na.rm = TRUE))
}

join_meta <- function(rhs,con = connect_quant()){
  quant_meta(con = con) %>%
    dplyr::ungroup() %>%
    dplyr::select(symbol, company, sector) %>%
    dplyr::distinct() -> meta_dat


  dplyr::left_join(meta_dat,rhs)
}

fill_database <- function(){

  library(furrr)
  sp50 <- tidyquant::tq_index("SP500")
  filt_ticks <- quant_table() %>%
    dplyr::distinct(retrieved_sys_date, symbol) %>%
    dplyr::filter(retrieved_sys_date %in% local(as.character(seq.Date(Sys.Date()-5,Sys.Date(),by = "day")))) %>%
    dplyr::collect() %>%
    dplyr::pull(symbol)


  if(nrow(sp50)>0){
    mydb <- connect_quant()
    sp50 <- sp50 %>%
      tibble::add_column(
        retrieved_date = Sys.time(),.before = 1) %>% tibble::add_column(
          retrieved_sys_date = Sys.Date(),.before = 1
        ) %>% dplyr::mutate_at(dplyr::vars(dplyr::contains("date")),funs(as.character)) %>%
      dplyr::mutate(retrieved_date = anytime::anytime(retrieved_date) %>% as.double())


    if(!("quant_meta" %in% dbListTables(mydb))){
      dbWriteTable(mydb,"quant_meta",sp50)
    }else{
      dbAppendTable(mydb,"quant_meta",sp50)
    }


    sp50 %>%
      # arrange(sector,desc(weight)) %>%
      # dplyr::group_by(sector) %>%
      # dplyr::filter(row_number() %in% 1:25) %>%
      dplyr::filter(!symbol %in% filt_ticks) %>%
      # dplyr::ungroup() %>%
      dplyr::pull(symbol) -> sp50_ticks


    plan(multiprocess)

    future_walk(sp50_ticks,~{
      get_quant_data_safe <- purrr::safely(get_quant_data)
      message(glue::glue("retrieving {.x}"))
      suppressMessages(get_quant_data_safe(.x)) %>% invisible()
    })
  }



}
