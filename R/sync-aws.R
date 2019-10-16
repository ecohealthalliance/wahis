#' get aws credentials
#' @importFrom aws.signature use_credentials
#' @noRd
get_aws_credentials <- function(){
    
    if (Sys.getenv("CI_JOB_ID") != "") {
        aws.signature::use_credentials(file = Sys.getenv("AWS_SIGNATURE_PATH"))
    } else {
        aws.signature::use_credentials()
    }
    
    Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
}


#' push to aws bucket
#' @param file files to compress
#' @import aws.s3
#' @importFrom here here
#' @noRd
push_aws <- function(folder, bucket){
    
    get_aws_credentials()
    
    # Compress the folder
    object <-  paste0(basename(folder), ".tar.xz")
    tar(tarfile = object,
        files = here::here(folder),
        compression = "xz",      # xz and level 9 makes this slow, but small!
        compression_level = 9,
        tar = "internal")
    
    if(!bucket_exists(bucket)[1]) {
        aws.s3::put_bucket(bucket = bucket, acl = "private")
    }
    
    # Upload the compressed file
    put_object(file = object,
               object = object,
               bucket = bucket,
               multipart = FALSE,
               verbose = FALSE)
    
    file.remove(object)
    
} 


#' pull from aws bucket
#' @param 
#' @import aws.s3
#' @importFrom here here
#' @importFrom fs dir_create dir_exists
#' @noRd
pull_aws <- function(bucket, object, dir = "."){
    
    get_aws_credentials()
    
    save_object(object = object,
                bucket = bucket,
                file = here::here(object),
                overwrite = TRUE)
    
    if(dir != "." && !dir_exists(dir)){
        dir_create(here::here(dir))
    }
    
    # Uncompress the file
    untar(object, tar = "internal", exdir = dir)
    file.remove(object)
    
}
