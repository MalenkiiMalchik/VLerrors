#' Writes files to Google Drive
#'
#' This function pulls in all relevant files from the database and stores them locally. It only works with an approved IP address.
#'
#' @param downpath Specify the path where downloaded files are saved
#' @importFrom magrittr %>%
#' @export

VLupload <- function(downpath){

  googledrive::drive_auth()

  for(n in 1:length(all_table_names)){
    tries = 0
    while(tries<3){
      test = try(googledrive::drive_upload(media = paste0(downpath, "/", all_table_names[n], ".csv"),
                                    name = paste0(all_table_names[n],".csv"),
                                    path = as_id("198YgVgnr9_z6YhqbQWbDlo0GJK70u_cV"),
                                    overwrite = TRUE))
      if(class(test)=="try-error"){tries = tries+1}
    }
  }
}
