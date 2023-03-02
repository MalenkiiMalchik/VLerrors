#' Cleans Abbott files
#'
#' Reads Abbott files, cleans data, and uploads them to Google Drive. Optionally returns one of two processed datasets.
#'
#' @param return Specify whether to return a dataset. Valid arguments are 'errors' and 'percent'. Defaults to FALSE
#' @param download Specify whether to download fresh files from Google Drive. Defaults to FALSE
#' @param data Specify where to download files to or where they are stored
#' @importFrom magrittr %>%
#' @export

VLabbott <- function(return = FALSE,
                    download = FALSE,
                    data) {
  #### LOAD DATA ============================================================================

  if (download == TRUE) {
    googledrive::drive_auth()

    folder <- "198YgVgnr9_z6YhqbQWbDlo0GJK70u_cV"

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "test_activity_abbott.csv",
      folderpath = data,
      zip = FALSE
    )

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "test_rate.csv",
      folderpath = data,
      zip = FALSE
    )

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "test.csv",
      folderpath = data,
      zip = FALSE
    )

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "instrument_master.csv",
      folderpath = data,
      zip = FALSE
    )
    glamr::import_drivefile(
      drive_folder = folder,
      filename = "error.csv",
      folderpath = data,
      zip = FALSE
    )

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "error_type.csv",
      folderpath = data,
      zip = FALSE
    )
    glamr::import_drivefile(
      drive_folder = folder,
      filename = "site.csv",
      folderpath = data,
      zip = FALSE
    )

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "test_type.csv",
      folderpath = data,
      zip = FALSE
    )
  }

  test_activity_abbott <-
    readr::read_csv(paste0(data, "/test_activity_abbott.csv"))

  test_rate <- readr::read_csv(paste0(data, "/test_rate.csv"))

  test = readr::read_csv(paste0(data, "/test.csv"))

  instrument_master = readr::read_csv(paste0(data, "/instrument_master.csv"))

  error = readr::read_csv(paste0(data, "/error.csv"))

  error_type = readr::read_csv(paste0(data, "/error_type.csv"))

  site = readr::read_csv(paste0(data, "/site.csv"))

  test_type = readr::read_csv(paste0(data, "/test_type.csv"))


  #### DATA WRANGLING ============================================================================

  instrument_master = instrument_master %>%
    dplyr::select(instrument_id, instrument_model, supplier_site_id) %>%
    dplyr::filter(instrument_id %in% unique(test_activity_abbott$instrument_id)) %>%
    dplyr::mutate(instrument_model = toupper(instrument_model))

  test_activity_abbott = test_activity_abbott %>%
    dplyr::left_join(instrument_master, by = c("instrument_id" = "instrument_id"))

  test_activity_abbott = test_activity_abbott %>%
    dplyr::mutate(instrument_model = stringr::str_remove_all(instrument_model, "®"))

  error_type = error_type %>%
    dplyr::select(error_type_id, error_type_name, error_type_description)

  site = site %>%
    dplyr::select(supplier_site_id, supplier_site_name)

  test_type = test_type %>%
    dplyr::select(test_type_id, test_type_psm)

  error_codes = test_activity_abbott %>%
    dplyr::left_join(test_type) %>%
    dplyr::filter(test_type_psm %in% c("EID", "EID/VL", "VL")) %>%
    dplyr::mutate(well_number = stringr::str_extract(test_record_id, "\\|\\d+$")) %>%
    dplyr::mutate(well_number = stringr::str_remove_all(well_number, "\\|")) %>%
    dplyr::mutate(test_record_id = stringr::str_remove(test_record_id, "\\|\\d+$")) %>%
    dplyr::filter(!is.na(error_type_id)) %>%
    dplyr::mutate(error_type_id = as.character(error_type_id)) %>%
    dplyr::left_join(error_type, by = c("error_type_id" = "error_type_id")) %>%
    dplyr::left_join(site, by = c("supplier_site_id" = "supplier_site_id")) %>%
    dplyr::select(
      instrument_id,
      instrument_model,
      supplier_site_id,
      supplier_site_name,
      country_code,
      well_number,
      test_type_id,
      test_type_psm,
      error_type_id,
      error_type_name,
      error_type_description,
      error_count,
      test_date,
      is_patient_test,
      is_control_test,
      patient_test_error_flag,
      control_test_error_flag,
      excluded_tests
    ) %>%
    dplyr::mutate(test_date = as.character(test_date)) %>%
    dplyr::mutate(test_month_year = format(lubridate::ymd(test_date), "%m-%Y"))

  if (return != "percent") {
    while (tries < 3) {
      test = try(googledrive::drive_upload(
        media = error_codes,
        name = paste0("error_codes_", Sys.Date(), ".csv"),
        path = googledrive::as_id("1PZGmTQ0fG4_zafM3JfTNL_ek-JeFifvw"),
        overwrite = TRUE
      ))
      if (class(test) == "try-error") {
        tries = tries + 1
      } else {
        break
      }
    }
  }
  if (return == "errors") {
    return(error_codes)
  }

  error_codes_percentages = test_activity_abbott %>%
    dplyr::mutate(well_number = stringr::str_extract(test_record_id, "\\|\\d+$")) %>%
    dplyr::mutate(well_number = stringr::str_remove_all(well_number, "\\|")) %>%
    dplyr::mutate(test_record_id = stringr::str_remove(test_record_id, "\\|\\d+$")) %>%
    dplyr::mutate(error_type_id = as.character(error_type_id)) %>%
    dplyr::left_join(error_type, by = c("error_type_id" = "error_type_id")) %>%
    dplyr::left_join(site, by = c("supplier_site_id" = "supplier_site_id"))

  # Obtain percent of plates affected by each code per month-year

  error_codes_errors = error_codes_percentages %>%
    dplyr::mutate(test_date = as.character(test_date)) %>%
    dplyr::mutate(test_month_year = format(lubridate::ymd(test_date), "%m-%Y")) %>%
    dplyr::filter(error_count > 0) %>%
    dplyr::group_by(
      test_record_id,
      test_month_year,
      instrument_model,
      supplier_site_name,
      country_code,
      error_type_id
    ) %>%
    dplyr::summarize(errors = n()) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(
      test_month_year,
      error_type_id,
      instrument_model,
      supplier_site_name,
      country_code
    ) %>%
    dplyr::summarize(plates_affected = n())

  error_codes_plates = error_codes_percentages %>%
    dplyr::mutate(test_date = as.character(test_date)) %>%
    dplyr::mutate(test_month_year = format(lubridate::ymd(test_date), "%m-%Y")) %>%
    dplyr::filter(!duplicated(test_record_id)) %>%
    dplyr::group_by(test_month_year) %>%
    dplyr::summarize(plates = n())

  abbott_error_percentages = error_codes_errors %>%
    dplyr::left_join(error_codes_plates) %>%
    dplyr::mutate(percentage = plates_affected * 100 / plates)

  if (return != "errors") {
    while (tries < 3) {
      test = try(googledrive::drive_upload(
        media = abbott_error_percentages,
        name = paste0("abbott_error_percentages_", Sys.Date(), ".csv"),
        path = googledrive::as_id("1PZGmTQ0fG4_zafM3JfTNL_ek-JeFifvw"),
        overwrite = TRUE
      ))
      if (class(test) == "try-error") {
        tries = tries + 1
      } else {
        break
      }
    }
  }
  if (return == "percent") {
    return(abbott_error_percentages)
  }

}
