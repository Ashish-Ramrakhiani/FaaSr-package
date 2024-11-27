#' @name faasr_s3_check
#' @title faasr_s3_check
#' @description 
#' Check 
#' 1. server's Endpoint&Region and Endpoint has a valid form(http).
#' 2. send a req for the list of buckets to check the status of s3 servers.
#' 3. Check that the bucket name exists.
#' @param faasr list with parsed and validated Payload
#' @return faasr list with parsed and validated payload
#' @importFrom "paws.storage" "s3"
#' @keywords internal

globalVariables(".faasr")

faasr_s3_check <- function(faasr){

  for(server in names(faasr$DataStores)){
    endpoint_check <- faasr$DataStores[[server]]$Endpoint
    region_check <- faasr$DataStores[[server]]$Region
    if (length(endpoint_check)==0 || endpoint_check=="") {
      faasr$DataStores[[server]]$Endpoint <- ""
    }else{
      if (!(startsWith(endpoint_check, "http"))){
        msg <- paste0('{\"faasr_s3_check\":\"Invalid Data store server endpoint ',server,'\"}', "\n")
        message(msg)
        stop()
      }
    }
    if (length(region_check)==0 || region_check==""){
      faasr$DataStores[[server]]$Region <- "us-east-1"
    }
    if (!is.null(faasr$DataStores[[server]]$Anonymous)){
      if (isTRUE(as.logical(faasr$DataStores[[server]]$Anonymous))){
        next
      }
    }
    s3<-paws.storage::s3(
      config=list(
	      credentials=list(
	        creds=list(
		        access_key_id=faasr$DataStores[[server]]$AccessKey,
		        secret_access_key=faasr$DataStores[[server]]$SecretKey
		      )
	      ),
	    endpoint=faasr$DataStores[[server]]$Endpoint,
	    region=faasr$DataStores[[server]]$Region
	  )
    )

    bucket_exists <- tryCatch({
    s3$head_bucket(Bucket = faasr$DataStores[[server]]$Bucket)
      TRUE
    }, error = function(e) {
      # Check if the error is due to bucket non-existence or access issues
      if (grepl("404|403", conditionMessage(e))) {
        FALSE
      } else {
        # Rethrow unexpected errors
        stop(e)
      }
    })
    
    # Handle bucket non-existence
    if (!bucket_exists) {
      msg <- paste0('{\"faasr_s3_check\":\"S3 server ',server,' failed with message: No such bucket or access denied\"}', "\n")
      message(msg)
      stop()
    }
  
  }
  return(faasr)
}
