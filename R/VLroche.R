#' Cleans Roche files
#'
#' Reads Roche files, cleans data, and uploads them to Google Drive. Optionally returns processed dataset.
#'
#' @param return Specify whether to return a dataset. Defaults to FALSE
#' @param download Specify whether to download fresh files from Google Drive. Defaults to FALSE
#' @param data Specify where to download files to or where they are stored
#' @param dataout Specify where cleaned datasets will be stored
#' @importFrom magrittr %>%
#' @export

VLroche <- function(return = FALSE,
                    download = FALSE,
                    data,
                    dataout) {
  if (download == TRUE) {
    googledrive::drive_auth()

    folder <- "198YgVgnr9_z6YhqbQWbDlo0GJK70u_cV"

    files_in_folder <- googledrive::drive_ls(googledrive::as_id(folder))

    glamr::import_drivefile(
      drive_folder = folder,
      filename = "test_activity_roche.csv",
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
  }

  test_activity_roche <-
    readr::read_csv(paste0(data, "/test_activity_roche.csv"))

  test_rate <- readr::read_csv(paste0(data, "/test_rate.csv"))

  test = readr::read_csv(paste0(data, "/test.csv"))

  instrument_master = readr::read_csv(paste0(data, "/instrument_master.csv"))

  error = readr::read_csv(paste0(data, "/error.csv"))

  error_type = readr::read_csv(paste0(data, "/error_type.csv"))

  site = readr::read_csv(paste0(data, "/site.csv"))


  #### DATA WRANGLING ============================================================================

  instrument_master = instrument_master %>%
    dplyr::select(instrument_id, instrument_model, supplier_site_id) %>%
    dplyr::mutate(instrument_id = as.numeric(instrument_id)) %>%
    dplyr::filter(instrument_id %in% unique(test_activity_roche$instrument_id)) %>%
    dplyr::mutate(instrument_model = toupper(instrument_model))

  test_activity_roche = test_activity_roche %>%
    dplyr::left_join(instrument_master, by = c("instrument_id" = "instrument_id"))

  test_activity_roche = test_activity_roche %>%
    dplyr::filter(stringr::str_detect(instrument_model, "00")) %>%
    dplyr::mutate(instrument_model = stringr::str_remove_all(instrument_model, "®"))


  error_codes = test_activity_roche %>%
    dplyr::mutate(internal_id = paste0("INTERNAL-", row_number())) %>%
    tidyr::separate(error_code_list,
                    c("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                    sep = ",") %>%
    tidyr::pivot_longer(cols = c("a", "b", "c", "d", "e", "f", "g", "h", "i"),
                        values_to = "error_code_list")

  error_codes = error_codes %>%
    dplyr::filter(name == "a" | !is.na(error_code_list))


  error_type = error_type %>%
    dplyr::filter(supplier_id == 1000 &
                    error_type_id %in% error_codes$error_code_list) %>%
    dplyr::select(error_type_id, error_type_description)

  site = site %>%
    dplyr::select(supplier_site_id, supplier_site_name)

  roche_errors = error_codes %>%
    dplyr::left_join(error_type, by = c("error_code_list" = "error_type_id")) %>%
    dplyr::left_join(site, by = c("supplier_site_id" = "supplier_site_id")) %>%
    dplyr::select(
      internal_id,
      name,
      error_code_list,
      error_type_description,
      instrument_id,
      instrument_model,
      supplier_site_id,
      supplier_site_name,
      country_code,
      test_type_id,
      test_date,
      is_patient_test,
      is_control_test,
      patient_test_error_flag,
      control_test_error_flag,
      excluded_test
    ) %>%
    dplyr::mutate(month = lubridate::floor_date(test_date, unit = "month")) %>%
    dplyr::mutate(test_month_year = zoo::as.yearmon(month)) %>%
    dplyr::select(-month) %>%
    dplyr::mutate(id_error = paste0(internal_id, "-", error_code_list)) %>%
    dplyr::mutate(dups = duplicated(id_error))

  # Filter out the duplicate errors

  roche_errors = roche_errors %>%
    dplyr::filter(dups == F) %>%
    dplyr::select(-id_error,-dups)

  roche_errors$error_type_description[roche_errors$error_code_list == "6282.36"] <-
    "Pipetted volume was not sufficient. Sample was not transferred"

  roche_errors %>%
    readr::write_csv(paste0(dataout, "/roche_errors.csv"))

  tries = 0
  while (tries < 3) {
    test = try(googledrive::drive_upload(
      media = paste0(dataout, "/roche_errors.csv"),
      name = paste0("roche_errors_", Sys.Date(), ".csv"),
      path = googledrive::as_id("1PZGmTQ0fG4_zafM3JfTNL_ek-JeFifvw"),
      overwrite = TRUE
    ))
    if (class(test) == "try-error") {
      tries = tries + 1
    } else {
      break
    }
  }

  if (return == TRUE) {
    return(roche_errors)
  }

}
