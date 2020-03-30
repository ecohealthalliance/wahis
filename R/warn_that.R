#' Support function to provide warnings in scripts (adapted from assertthat)
#' @importFrom assertthat see_if
#' @noRd

warn_that <- function(..., env = parent.frame(), msg = NULL){
    
    res <- see_if(..., env = env, msg = msg)
    if (res) 
        return(TRUE)
    warning(attr(res, "msg"))
    
}
