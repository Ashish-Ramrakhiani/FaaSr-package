#' @name faasr_vm_orchestrate
#' @title Main VM orchestration entry point
#' @description Handles VM lifecycle transparently based on strategy
#' @param faasr FaaSr configuration list
#' @export
faasr_vm_orchestrate <- function(.faasr) {
  
  # Check if workflow has any VM requirements
  if (!faasr_workflow_needs_vm(.faasr)) {
    # No VM needed - execute normally
    return(faasr_execute_regular_function(.faasr))
  }
  
  # Get VM strategy from configuration
  vm_strategy <- faasr_get_vm_strategy(.faasr)
  
  # Dispatch to strategy-specific handler
  result <- switch(vm_strategy,
                   "simple_start_end" = faasr_execute_strategy_simple_start_end(.faasr),
                   "per_function" = faasr_execute_strategy_per_function(.faasr),      # Future: Strategy 2
                   "optimized" = faasr_execute_strategy_optimized(.faasr),           # Future: Strategy 3
                   stop(paste("Unknown VM strategy:", vm_strategy))
  )
  
  return(result)
}

#' @name faasr_workflow_needs_vm
#' @title Check if any function in workflow requires VM
#' @param faasr FaaSr configuration list
#' @export
faasr_workflow_needs_vm <- function(.faasr) {
  
  for (func_name in names(faasr$FunctionList)) {
    func_config <- faasr$FunctionList[[func_name]]
    if (!is.null(func_config$RequiresVM) && func_config$RequiresVM == TRUE) {
      return(TRUE)
    }
  }
  return(FALSE)
}

#' @name faasr_get_vm_strategy
#' @title Get VM orchestration strategy from configuration
#' @param faasr FaaSr configuration list
#' @export
faasr_get_vm_strategy <- function(.faasr) {
  
  # Default to simple start/end strategy
  default_strategy <- "simple_start_end"
  
  if (is.null(faasr$VMConfig)) {
    return(default_strategy)
  }
  
  strategy <- faasr$VMConfig$Strategy %||% default_strategy
  return(strategy)
}

#' @name faasr_execute_strategy_simple_start_end
#' @title Execute Strategy 1: VM start at first function, stop at last function
#' @param faasr FaaSr configuration list
#' @export
faasr_execute_strategy_simple_start_end <- function(.faasr) {
  
  current_function <- faasr$FunctionInvoke
  
  # Determine function position in workflow
  function_position <- faasr_get_function_position_in_workflow(.faasr, current_function)
  
  log_msg <- paste0("Strategy 1 - Function: ", current_function, 
                    ", Position: ", function_position$type)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # STEP 1: Handle VM start (only for first function)
  if (function_position$is_first) {
    log_msg <- "Strategy 1: Starting VM (runner service will auto-start)"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    
    vm_details <- faasr_vm_start(.faasr)
    faasr_vm_wait_ready(.faasr, vm_details)
    
    log_msg <- "VM started - GitHub Actions runner service is now available"
    faasr_log(log_msg)
  }
  
  # STEP 2: Execute the function
  if (!is.null(faasr$FunctionList[[current_function]]$RequiresVM) && 
      faasr$FunctionList[[current_function]]$RequiresVM == TRUE) {
    
    log_msg <- paste0("Function ", current_function, " will execute on self-hosted runner (VM)")
    faasr_log(log_msg)
    cat(log_msg, "\n")
  } else {
    log_msg <- paste0("Function ", current_function, " will execute on GitHub-hosted runner")
    faasr_log(log_msg)
    cat(log_msg, "\n")
  }
  
  # Execute user function (GitHub Actions handles runner routing automatically)
  .faasr <- faasr_run_user_function(.faasr)
  
  # STEP 3: Handle VM stop (only for last function)
  if (function_position$is_last) {
    log_msg <- "Strategy 1: Terminating VM (runner service will auto-stop)"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    
    faasr_vm_terminate(.faasr)
  }
  
  return(.faasr)
}

#' @name faasr_get_function_position_in_workflow
#' @title Determine if function is first, last, or middle in workflow
#' @param faasr FaaSr configuration list
#' @param current_function Name of current function
#' @export
faasr_get_function_position_in_workflow <- function(.faasr, current_function) {
  
  workflow <- faasr$FunctionList
  entry_point <- faasr$FunctionInvoke
  
  # Check if this is the entry point (first function)
  is_first <- (current_function == entry_point)
  
  # Check if this is a terminal function (no InvokeNext or empty InvokeNext)
  current_config <- workflow[[current_function]]
  has_next <- !is.null(current_config$InvokeNext) && length(current_config$InvokeNext) > 0
  is_last <- !has_next
  
  # Determine function type
  if (is_first && is_last) {
    function_type <- "single"  # Only one function in workflow
  } else if (is_first) {
    function_type <- "first"
  } else if (is_last) {
    function_type <- "last"
  } else {
    function_type <- "middle"
  }
  
  return(list(
    type = function_type,
    is_first = is_first,
    is_last = is_last,
    current_function = current_function
  ))
}

#' @name faasr_execute_regular_function
#' @title Execute function without VM orchestration
#' @param faasr FaaSr configuration list
#' @export
faasr_execute_regular_function <- function(.faasr) {
  
  log_msg <- paste0("Executing regular function: ", faasr$FunctionInvoke)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Standard FaaSr execution
  .faasr <- faasr_run_user_function(.faasr)
  return(.faasr)
}

# PLACEHOLDER IMPLEMENTATIONS FOR FUTURE STRATEGIES
faasr_execute_strategy_per_function <- function(.faasr) {
  stop("Strategy 2 (per_function) not yet implemented")
}

faasr_execute_strategy_optimized <- function(.faasr) {
  stop("Strategy 3 (optimized) not yet implemented")
}

#' @name faasr_vm_start
#' @title Start VM instance
#' @param faasr FaaSr configuration list
#' @export
faasr_vm_start <- function(.faasr) {
  
  if (is.null(faasr$VMConfig)) {
    stop("VMConfig not found in workflow configuration")
  }
  
  vm_config <- faasr$VMConfig
  faasr_validate_vm_config(vm_config)
  
  log_msg <- paste0("Starting VM: ", vm_config$Provider, " ", vm_config$InstanceType)
  faasr_log(log_msg)
  
  # Dispatch to cloud provider - FIXED: Pass both vm_config AND faasr
  vm_details <- switch(vm_config$Provider,
                       "AWS" = faasr_aws_start_vm(vm_config, .faasr),
                       stop(paste("Unsupported VM provider:", vm_config$Provider))
  )
  
  # Store VM state for later cleanup
  faasr_vm_put_state(.faasr, vm_details)
  
  log_msg <- paste0("VM started: ", vm_details$InstanceId)
  faasr_log(log_msg)
  
  return(vm_details)
}

#' @name faasr_vm_terminate
#' @title Terminate VM instance
#' @param faasr FaaSr configuration list
#' @export
faasr_vm_terminate <- function(.faasr) {
  
  # Get VM state
  vm_details <- faasr_vm_get_state(.faasr)
  
  if (is.null(vm_details)) {
    log_msg <- "No VM state found - VM may already be terminated"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    return(TRUE)
  }
  
  log_msg <- paste0("Terminating VM: ", vm_details$InstanceId)
  faasr_log(log_msg)
  
  # Dispatch to cloud provider
  success <- switch(vm_details$Provider,
                    "AWS" = faasr_aws_terminate_vm(vm_details, .faasr$VMConfig),
                    stop(paste("Unsupported VM provider:", vm_details$Provider))
  )
  
  if (success) {
    faasr_vm_delete_state(.faasr)
    log_msg <- paste0("VM terminated successfully: ", vm_details$InstanceId)
    faasr_log(log_msg)
  }
  
  return(success)
}

#' @name faasr_vm_wait_ready
#' @title Wait for VM to be ready
#' @param faasr FaaSr configuration list
#' @param vm_details VM details from start operation
#' @export
faasr_vm_wait_ready <- function(.faasr, vm_details) {
  
  max_wait_time <- 300  # 5 minutes
  check_interval <- 30  # 30 seconds
  start_time <- Sys.time()
  
  log_msg <- "Waiting for VM to be ready..."
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  while (TRUE) {
    elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    
    if (elapsed_time > max_wait_time) {
      stop("VM failed to become ready within timeout period")
    }
    
    # Simple wait - in production you'd check actual VM status
    if (elapsed_time > 120) {  # Wait at least 2 minutes
      log_msg <- "VM should be ready"
      faasr_log(log_msg)
      cat(log_msg, "\n")
      break
    }
    
    cat(".")
    Sys.sleep(check_interval)
  }
}

# ------------------------------------------------------------------------------
# VM STATE MANAGEMENT (S3-based)
# ------------------------------------------------------------------------------

#' @name faasr_vm_put_state
#' @title Store VM state in S3
#' @param faasr FaaSr configuration list
#' @param vm_details VM details to store
#' @export
faasr_vm_put_state <- function(.faasr, vm_details) {
  
  # Add metadata
  vm_state <- list(
    vm_details = vm_details,
    workflow_id = .faasr$InvocationID,
    strategy = faasr_get_vm_strategy(.faasr),
    created_at = Sys.time(),
    Provider = .faasr$VMConfig$Provider
  )
  
  state_file <- paste0(.faasr$InvocationID, "_vm_state.json")
  vm_json <- jsonlite::toJSON(vm_state, auto_unbox = TRUE, pretty = TRUE)
  
  temp_file <- tempfile(fileext = ".json")
  writeLines(vm_json, temp_file)
  
  faasr_put_file(
    remote_folder = "FaaSrVM",
    remote_file = state_file,
    local_file = basename(temp_file),
    local_folder = dirname(temp_file)
  )
  
  unlink(temp_file)
}

#' @name faasr_vm_get_state
#' @title Retrieve VM state from S3
#' @param faasr FaaSr configuration list
#' @export
faasr_vm_get_state <- function(.faasr) {
  
  state_file <- paste0(faasr$InvocationID, "_vm_state.json")
  temp_file <- tempfile(fileext = ".json")
  
  tryCatch({
    faasr_get_file(
      remote_folder = "FaaSrVM",
      remote_file = state_file,
      local_file = basename(temp_file),
      local_folder = dirname(temp_file)
    )
    
    vm_json <- readLines(temp_file)
    vm_state <- jsonlite::fromJSON(paste(vm_json, collapse = "\n"))
    unlink(temp_file)
    
    return(vm_state$vm_details)
    
  }, error = function(e) {
    if (file.exists(temp_file)) unlink(temp_file)
    return(NULL)
  })
}

#' @name faasr_vm_delete_state
#' @title Delete VM state from S3
#' @param faasr FaaSr configuration list
#' @export
faasr_vm_delete_state <- function(.faasr) {
  state_file <- paste0(faasr$InvocationID, "_vm_state.json")
  faasr_delete_file(remote_folder = "FaaSrVM", remote_file = state_file)
}

#' @name faasr_aws_start_vm
#' @title Start AWS EC2 instance with FaaSr credential pattern
#' @param vm_config VM configuration
#' @importFrom paws.compute ec2
#' @export
faasr_aws_start_vm <- function(vm_config, faasr = NULL) {
  
  # CORRECTED: Follow DataStore credential pattern - get from environment
  aws_access_key <- Sys.getenv(vm_config$AccessKey)
  aws_secret_key <- Sys.getenv(vm_config$SecretKey)
  
  if (aws_access_key == "" || aws_secret_key == "") {
    stop(paste("AWS credentials not found in environment variables:", 
               vm_config$AccessKey, "or", vm_config$SecretKey))
  }
  
  log_msg <- paste0("Using AWS credentials from environment variable: ", vm_config$AccessKey)
  faasr_log(log_msg)
  
  # Create EC2 client
  ec2 <- paws.compute::ec2(
    config = list(
      credentials = list(
        accessKeyId = aws_access_key,
        secretAccessKey = aws_secret_key
      ),
      region = vm_config$Region
    )
  )
  
  # Get workflow ID for tagging
  workflow_id <- if (!is.null(faasr)) faasr$InvocationID else "unknown"
  
  # Start instance
  result <- ec2$run_instances(list(
    ImageId = vm_config$AMI_ID,
    MinCount = 1L,
    MaxCount = 1L,
    InstanceType = vm_config$InstanceType,
    KeyName = vm_config$KeyName %||% NULL,
    SecurityGroupIds = vm_config$SecurityGroupIds %||% NULL,
    SubnetId = vm_config$SubnetId %||% NULL,
    TagSpecifications = list(
      list(
        ResourceType = "instance",
        Tags = list(
          list(Key = "Name", Value = "FaaSr-VM-AutoRunner"),
          list(Key = "Purpose", Value = "FaaSr-GitHub-Runner"),
          list(Key = "CreatedBy", Value = "FaaSr"),
          list(Key = "WorkflowID", Value = workflow_id)
        )
      )
    )
  ))
  
  return(result$Instances[[1]])
}

#' @name faasr_aws_terminate_vm
#' @title Terminate AWS EC2 instance with FaaSr credential pattern
#' @param vm_details VM details with instance information
#' @param vm_config VM configuration for credentials
#' @export
faasr_aws_terminate_vm <- function(vm_details, vm_config) {
  
  aws_access_key <- Sys.getenv(vm_config$AccessKey)
  aws_secret_key <- Sys.getenv(vm_config$SecretKey)
  
  if (aws_access_key == "" || aws_secret_key == "") {
    stop(paste("AWS credentials not found in environment variables:", 
               vm_config$AccessKey, "or", vm_config$SecretKey))
  }
  
  # Extract region from instance placement
  region <- vm_details$Placement$AvailabilityZone
  # Remove availability zone suffix (e.g., "us-west-2a" -> "us-west-2")
  region <- substr(region, 1, nchar(region) - 1)
  
  ec2 <- paws.compute::ec2(
    config = list(
      credentials = list(
        accessKeyId = aws_access_key,
        secretAccessKey = aws_secret_key
      ),
      region = region
    )
  )
  
  # Terminate instance
  result <- ec2$terminate_instances(list(InstanceIds = list(vm_details$InstanceId)))
  
  return(length(result$TerminatingInstances) > 0)
}

#' @name faasr_validate_vm_config
#' @title Validate VM configuration with FaaSr credential pattern
#' @param vm_config VM configuration
#' @export
faasr_validate_vm_config <- function(vm_config) {
  
  required_fields <- c("Provider", "InstanceType", "Region")
  
  for (field in required_fields) {
    if (is.null(vm_config[[field]]) || vm_config[[field]] == "") {
      stop(paste("Required VM config field missing:", field))
    }
  }
  
  if (vm_config$Provider == "AWS") {
    aws_required <- c("AMI_ID", "AccessKey", "SecretKey")
    for (field in aws_required) {
      if (is.null(vm_config[[field]])) {
        stop(paste("Required AWS VM config field missing:", field))
      }
    }
  }
  
  return(TRUE)
}

# Helper operator
`%||%` <- function(x, y) if (is.null(x)) y else x