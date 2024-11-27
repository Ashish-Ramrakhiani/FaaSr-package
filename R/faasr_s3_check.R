faasr_s3_check <- function(faasr){
  for(server in names(faasr$DataStores)){
    # Verbose debugging
    cat("Checking server:", server, "\n")
    cat("Endpoint:", faasr$DataStores[[server]]$Endpoint, "\n")
    cat("Region:", faasr$DataStores[[server]]$Region, "\n")
    cat("Bucket:", faasr$DataStores[[server]]$Bucket, "\n")
    
    endpoint_check <- faasr$DataStores[[server]]$Endpoint
    region_check <- faasr$DataStores[[server]]$Region
    
    # Endpoint validation
    if (length(endpoint_check)==0 || endpoint_check=="") {
      faasr$DataStores[[server]]$Endpoint <- ""
    } else {
      if (!(startsWith(endpoint_check, "http"))){
        msg <- paste0('{\"faasr_s3_check\":\"Invalid Data store server endpoint ',server,'\"}', "\n")
        message(msg)
        stop()
      }
    }
    
    # Region handling
    if (length(region_check)==0 || region_check==""){
      faasr$DataStores[[server]]$Region <- "us-east-1"
    }
    
    # Skip anonymous access
    if (!is.null(faasr$DataStores[[server]]$Anonymous)){
      if (isTRUE(as.logical(faasr$DataStores[[server]]$Anonymous))){
        cat("Skipping anonymous access server\n")
        next
      }
    }
    
    # Enhanced error handling for S3 connection
    tryCatch({
      s3 <- paws.storage::s3(
        config = list(
          credentials = list(
            creds = list(
              access_key_id = faasr$DataStores[[server]]$AccessKey,
              secret_access_key = faasr$DataStores[[server]]$SecretKey
            )
          ),
          endpoint = faasr$DataStores[[server]]$Endpoint,
          region = faasr$DataStores[[server]]$Region
        )
      )
      
      # Detailed bucket existence check
      bucket_exists <- tryCatch({
        result <- s3$head_bucket(Bucket = faasr$DataStores[[server]]$Bucket)
        cat("Bucket head operation successful\n")
        TRUE
      }, error = function(e) {
        # Detailed error logging
        cat("Bucket check error:\n")
        cat("Error message:", conditionMessage(e), "\n")
        cat("Error class:", class(e), "\n")
        
        # Check specific error conditions
        if (grepl("404|403", conditionMessage(e))) {
          cat("Bucket does not exist or access denied\n")
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
    
    }, error = function(e) {
      # Catch and log any errors during S3 client creation
      cat("S3 client creation error:\n")
      cat("Error message:", conditionMessage(e), "\n")
      stop(paste("Failed to create S3 client for server:", server))
    })
  }
  return(faasr)
}
