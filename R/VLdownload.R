#' Pull Files from Database
#'
#' This function pulls in all relevant files from the database and stores them locally. It only works with an approved IP address.
#'
#' @param dsn_database Specify the name of your database.
#' @param dsn_hostname Specify the hostname
#' @param dsn_port Specify your port number as a character. e.g. 5432
#' @param dsn_uid Specify your username. e.g. "admin"
#' @param dsn_pwd Specify your password
#' @param downpath Specify the path to save downloaded files in
#' @importFrom magrittr %>%
#' @export

VLdownload <- function(dsn_database,
                       dsn_hostname,
                       dsn_port,
                       dsn_uid,
                       dsn_pwd,
                       downpath){

  connec <- DBI::dbConnect(RPostgres::Postgres(),
                           dbname = dsn_database,
                           host = dsn_hostname,
                           port = dsn_port,
                           user = dsn_uid,
                           password = dsn_pwd)

  vldata_tables_to_download <- DBI::dbGetQuery(connec, "SELECT table_name FROM information_schema.tables
                         WHERE table_schema='vldata'")

  stgdata_tables_to_download <- DBI::dbGetQuery(connec, "SELECT table_name FROM information_schema.tables
                         WHERE table_schema='stgdata'")

  vldata_tables <- purrr::map(.x = vldata_tables_to_download$table_name, ~ dbGetQuery(connec, paste0("SELECT * FROM vlprddb.vldata.",.)))
  stgdata_tables <- purrr::map(.x = stgdata_tables_to_download$table_name, ~ dbGetQuery(connec, paste0("SELECT * FROM vlprddb.stgdata.",.)))

  DBI::dbDisconnect(connec)

  # combined lists containing all our data from vldata and staging schemas
  all_tables <- c(vldata_tables, stgdata_tables)

  # combine all table names
  all_table_names <- c(vldata_tables_to_download$table_name , stgdata_tables_to_download$table_name)

  # save files to csv's (we can't directly save a csv in Google Drive from a data frame)
  purrr::walk(.x = 1:length(all_tables), ~ readr::write_csv(all_tables[[.]], file = paste0(downpath, "/", all_table_names[.],".csv")))

  return(all_table_names)
}
