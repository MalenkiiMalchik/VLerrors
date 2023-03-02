#' Cleans Abbott files
#'
#' Reads Abbott files, cleans data, and uploads them to Google Drive. Optionally returns one of two processed datasets.
#'
#' @param return Specify whether to return a dataset. Valid arguments are 'errors' and 'percent'
#' @importFrom magrittr %>%
#' @export

VLabbot <- function(return){

  #### LOAD DATA ============================================================================


  drive_auth()

  folder <- "198YgVgnr9_z6YhqbQWbDlo0GJK70u_cV"

  files_in_folder <- googledrive::drive_ls(googledrive::as_id(folder))

  glamr::import_drivefile(drive_folder = folder,
                          filename = "test_activity_abbott.csv",
                          folderpath = data,
                          zip = FALSE)

  glamr::import_drivefile(drive_folder = folder,
                          filename = "test_rate.csv",
                          folderpath = data,
                          zip = FALSE)

  glamr::import_drivefile(drive_folder = folder,
                          filename = "test.csv",
                          folderpath = data,
                          zip = FALSE)

  glamr::import_drivefile(drive_folder = folder,
                          filename = "instrument_master.csv",
                          folderpath = data,
                          zip = FALSE)
  glamr::import_drivefile(drive_folder = folder,
                          filename = "error.csv",
                          folderpath = data,
                          zip = FALSE)

  glamr::import_drivefile(drive_folder = folder,
                          filename = "error_type.csv",
                          folderpath = data,
                          zip = FALSE)
  glamr::import_drivefile(drive_folder = folder,
                          filename = "site.csv",
                          folderpath = data,
                          zip = FALSE)
  glamr::import_drivefile(drive_folder = folder,
                          filename = "test_type.csv",
                          folderpath = data,
                          zip = FALSE)


  test_activity_abbott <- read_csv(here("Data", "test_activity_abbott.csv"))

  test_rate <- read_csv(here("Data", "test_rate.csv"))

  test = read_csv(here("Data", "test.csv"))

  instrument_master = read_csv(here("Data", "instrument_master.csv"))

  error = read_csv(here("Data", "error.csv"))

  error_type = read_csv(here("Data", "error_type.csv"))

  site = read_csv(here("Data", "site.csv"))

  test_type = read_csv(here("Data", "test_type.csv"))


  #### DATA WRANGLING ============================================================================

  instrument_master = instrument_master %>%
    select(instrument_id, instrument_model, supplier_site_id) %>%
    filter(instrument_id %in% unique(test_activity_abbott$instrument_id)) %>%
    mutate(instrument_model = toupper(instrument_model))

  test_activity_abbott = test_activity_abbott %>%
    left_join(instrument_master, by = c("instrument_id" = "instrument_id"))

  test_activity_abbott = test_activity_abbott %>%
    mutate(instrument_model = str_remove_all(instrument_model, "®"))

  error_type = error_type %>%
    select(error_type_id, error_type_name, error_type_description)

  site = site %>%
    select(supplier_site_id, supplier_site_name)

  test_type = test_type %>%
    select(test_type_id, test_type_psm)

  error_codes = test_activity_abbott %>%
    left_join(test_type) %>%
    filter(test_type_psm %in% c("EID", "EID/VL", "VL")) %>%
    mutate(well_number = str_extract(test_record_id, "\\|\\d+$")) %>%
    mutate(well_number = str_remove_all(well_number, "\\|")) %>%
    mutate(test_record_id = str_remove(test_record_id, "\\|\\d+$")) %>%
    filter(!is.na(error_type_id)) %>%
    mutate(error_type_id = as.character(error_type_id)) %>%
    left_join(error_type, by = c("error_type_id" = "error_type_id")) %>%
    left_join(site, by = c("supplier_site_id" = "supplier_site_id")) %>%
    select(instrument_id,
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
    mutate(test_date = as.character(test_date)) %>%
    mutate(test_month_year = format(ymd(test_date), "%m-%Y"))

  if(return != "percent"){
    while(tries<3){
      test = try(googledrive::drive_upload(media = error_codes,
                                           name = paste0("error_codes_", Sys.Date(), ".csv"),
                                           path = as_id("1PZGmTQ0fG4_zafM3JfTNL_ek-JeFifvw"),
                                           overwrite = TRUE))
      if(class(test)=="try-error"){
        tries = tries+1
        } else {break}
    }
  }
  if(return == "errors"){
    return(error_codes)
  }

  error_codes_percentages = test_activity_abbott %>%
    mutate(well_number = str_extract(test_record_id, "\\|\\d+$")) %>%
    mutate(well_number = str_remove_all(well_number, "\\|")) %>%
    mutate(test_record_id = str_remove(test_record_id, "\\|\\d+$")) %>%
    mutate(error_type_id = as.character(error_type_id)) %>%
    left_join(error_type, by = c("error_type_id" = "error_type_id")) %>%
    left_join(site, by = c("supplier_site_id" = "supplier_site_id"))

  # Obtain percent of plates affected by each code per month-year

  error_codes_errors = error_codes_percentages %>%
    mutate(test_date = as.character(test_date)) %>%
    mutate(test_month_year = format(ymd(test_date), "%m-%Y")) %>%
    filter(error_count>0) %>%
    group_by(test_record_id,
             test_month_year,
             instrument_model,
             supplier_site_name,
             country_code,
             error_type_id) %>%
    summarize(errors = n()) %>%
    ungroup() %>%
    group_by(test_month_year,
             error_type_id,
             instrument_model,
             supplier_site_name,
             country_code) %>%
    summarize(plates_affected = n())

  error_codes_plates = error_codes_percentages %>%
    mutate(test_date = as.character(test_date)) %>%
    mutate(test_month_year = format(ymd(test_date), "%m-%Y")) %>%
    filter(!duplicated(test_record_id)) %>%
    group_by(test_month_year) %>%
    summarize(plates = n())

  abbott_error_percentages = error_codes_errors %>%
    left_join(error_codes_plates) %>%
    mutate(percentage = plates_affected*100/plates)

  if(return != "errors"){
    while(tries<3){
      test = try(googledrive::drive_upload(media = abbott_error_percentages,
                                           name = paste0("abbott_error_percentages_", Sys.Date(), ".csv"),
                                           path = as_id("1PZGmTQ0fG4_zafM3JfTNL_ek-JeFifvw"),
                                           overwrite = TRUE))
      if(class(test)=="try-error"){
        tries = tries+1
        } else {break}
    }
  }
  if(return == "percent"){
    return(abbott_error_percentages)
  }

}
