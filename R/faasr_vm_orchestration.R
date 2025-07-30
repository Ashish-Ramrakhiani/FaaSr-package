#' @name faasr_vm_orchestrate
#' @title Main VM orchestration entry point
#' @description Handles VM lifecycle transparently based on strategy
#' @param .faasr FaaSr configuration list
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
                   "simple_start_end" = faasr_execute_strategy_simple_start_end_fixed(.faasr),  # CHANGED: Use fixed version
                   "per_function" = faasr_execute_strategy_per_function(.faasr),      # Future: Strategy 2
                   "optimized" = faasr_execute_strategy_optimized(.faasr),           # Future: Strategy 3
                   stop(paste("Unknown VM strategy:", vm_strategy))
  )
  
  return(result)
}

#' @name faasr_workflow_needs_vm
#' @title Check if any function in workflow requires VM
#' @param .faasr FaaSr configuration list
#' @export
faasr_workflow_needs_vm <- function(.faasr) {
  
  for (func_name in names(.faasr$FunctionList)) {
    func_config <- .faasr$FunctionList[[func_name]]
    if (!is.null(func_config$RequiresVM) && func_config$RequiresVM == TRUE) {
      return(TRUE)
    }
  }
  return(FALSE)
}

#' @name faasr_get_vm_strategy
#' @title Get VM orchestration strategy from configuration
#' @param .faasr FaaSr configuration list
#' @export
faasr_get_vm_strategy <- function(.faasr) {
  
  # Default to simple start/end strategy
  default_strategy <- "simple_start_end"
  
  if (is.null(.faasr$VMConfig)) {
    return(default_strategy)
  }
  
  strategy <- .faasr$VMConfig$Strategy %||% default_strategy
  return(strategy)
}

#' @name faasr_execute_strategy_simple_start_end_fixed
#' @title Execute Strategy 1: Fixed version using existing instance
#' @param .faasr FaaSr configuration list
#' @export
faasr_execute_strategy_simple_start_end_fixed <- function(.faasr) {
  
  current_function <- .faasr$FunctionInvoke
  
  # Determine function position in workflow
  function_position <- faasr_get_function_position_in_workflow(.faasr, current_function)
  
  log_msg <- paste0("Strategy 1 (Fixed) - Function: ", current_function, 
                    ", Position: ", function_position$type)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # STEP 1: Handle VM start (only for first function)
  if (function_position$is_first) {
    log_msg <- "Strategy 1: Starting existing VM instance (runner will auto-start)"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    
    vm_details <- faasr_vm_start_simplified(.faasr)
    faasr_vm_wait_ready_simplified(.faasr, vm_details)
    
    log_msg <- "Existing VM started - GitHub Actions runner service should be available"
    faasr_log(log_msg)
  }
  
  # STEP 2: Execute the function
  if (!is.null(.faasr$FunctionList[[current_function]]$RequiresVM) && 
      .faasr$FunctionList[[current_function]]$RequiresVM == TRUE) {
    
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
    log_msg <- "Strategy 1: Stopping existing VM instance (runner will auto-stop)"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    
    faasr_vm_stop_simplified(.faasr)
  }
  
  return(.faasr)
}

#' @name faasr_get_function_position_in_workflow
#' @title Determine if function is first, last, or middle in workflow
#' @param .faasr FaaSr configuration list
#' @param current_function Name of current function
#' @export
faasr_get_function_position_in_workflow <- function(.faasr, current_function) {
  
  workflow <- .faasr$FunctionList
  entry_point <- .faasr$FunctionInvoke
  
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
#' @param .faasr FaaSr configuration list
#' @export
faasr_execute_regular_function <- function(.faasr) {
  
  log_msg <- paste0("Executing regular function: ", .faasr$FunctionInvoke)
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

#' @name faasr_vm_start_simplified
#' @title Start VM instance - simplified version without S3 state
#' @param .faasr FaaSr configuration list
#' @export
faasr_vm_start_simplified <- function(.faasr) {
  
  if (is.null(.faasr$VMConfig)) {
    stop("VMConfig not found in workflow configuration")
  }
  
  vm_config <- .faasr$VMConfig
  faasr_validate_vm_config_existing(vm_config)
  
  log_msg <- paste0("Starting existing VM: ", vm_config$Provider, " instance ", vm_config$InstanceId)
  faasr_log(log_msg)
  
  # Dispatch to cloud provider
  vm_details <- switch(vm_config$Provider,
                       "AWS" = faasr_aws_start_existing_vm(vm_config, .faasr),
                       stop(paste("Unsupported VM provider:", vm_config$Provider))
  )
  
  log_msg <- paste0("VM start initiated: ", vm_details$InstanceId)
  faasr_log(log_msg)
  
  return(vm_details)
}

#' @name faasr_vm_stop_simplified
#' @title Stop VM instance - simplified version without S3 state
#' @param .faasr FaaSr configuration list
#' @export
faasr_vm_stop_simplified <- function(.faasr) {
  
  if (is.null(.faasr$VMConfig)) {
    log_msg <- "No VMConfig found - skipping VM stop"
    faasr_log(log_msg)
    cat(log_msg, "\n")
    return(TRUE)
  }
  
  vm_config <- .faasr$VMConfig
  
  log_msg <- paste0("Stopping VM: ", vm_config$InstanceId)
  faasr_log(log_msg)
  
  # Dispatch to cloud provider
  success <- switch(vm_config$Provider,
                    "AWS" = faasr_aws_stop_existing_vm(vm_config),
                    stop(paste("Unsupported VM provider:", vm_config$Provider))
  )
  
  if (success) {
    log_msg <- paste0("VM stop initiated successfully: ", vm_config$InstanceId)
    faasr_log(log_msg)
  }
  
  return(success)
}

#' @name faasr_vm_wait_ready_simplified
#' @title Wait for VM to be ready - simplified version
#' @param .faasr FaaSr configuration list
#' @param vm_details VM details from start operation
#' @export
faasr_vm_wait_ready_simplified <- function(.faasr, vm_details) {
  
  max_wait_time <- 180  # 3 minutes (reduced since existing instance starts faster)
  check_interval <- 15  # 15 seconds
  start_time <- Sys.time()
  
  log_msg <- "Waiting for existing VM to be ready..."
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # For existing instances with auto-start runner, shorter wait time
  while (TRUE) {
    elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    
    if (elapsed_time > max_wait_time) {
      log_msg <- "VM wait timeout reached - proceeding (runner may auto-start)"
      faasr_log(log_msg)
      cat(log_msg, "\n")
      break
    }
    
    # Shorter wait for existing instances
    if (elapsed_time > 60) {  # Wait at least 1 minute
      log_msg <- "VM should be ready - runner service auto-starting"
      faasr_log(log_msg)
      cat(log_msg, "\n")
      break
    }
    
    cat(".")
    Sys.sleep(check_interval)
  }
}

# ------------------------------------------------------------------------------
# AWS-SPECIFIC VM FUNCTIONS - UPDATED FOR EXISTING INSTANCES
# ------------------------------------------------------------------------------

#' @name faasr_aws_start_existing_vm
#' @title Start existing AWS EC2 instance (replaces faasr_aws_start_vm)
#' @param vm_config VM configuration including InstanceId
#' @param .faasr FaaSr configuration list
#' @importFrom paws.compute ec2
#' @export
faasr_aws_start_existing_vm <- function(vm_config, .faasr = NULL) {
  
  # Validate required fields for existing instance
  if (is.null(vm_config$InstanceId) || vm_config$InstanceId == "") {
    stop("InstanceId is required in VMConfig for existing instance strategy")
  }
  
  # Get AWS credentials from environment (following DataStore pattern)
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
  
  log_msg <- paste0("Starting existing instance: ", vm_config$InstanceId)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Start the existing instance
  tryCatch({
    result <- ec2$start_instances(list(InstanceIds = list(vm_config$InstanceId)))
    
    if (length(result$StartingInstances) > 0) {
      instance_info <- result$StartingInstances[[1]]
      log_msg <- paste0("Instance ", vm_config$InstanceId, " starting. Current state: ", 
                        instance_info$CurrentState$Name)
      faasr_log(log_msg)
      cat(log_msg, "\n")
      
      # Return simplified instance details for compatibility
      return(list(
        InstanceId = vm_config$InstanceId,
        State = instance_info$CurrentState$Name,
        Provider = "AWS"
      ))
    } else {
      stop("Failed to start instance - no instances returned")
    }
  }, error = function(e) {
    stop(paste("Failed to start instance", vm_config$InstanceId, ":", e$message))
  })
}

#' @name faasr_aws_stop_existing_vm
#' @title Stop existing AWS EC2 instance (replaces faasr_aws_terminate_vm)
#' @param vm_config VM configuration including InstanceId
#' @export
faasr_aws_stop_existing_vm <- function(vm_config) {
  
  aws_access_key <- Sys.getenv(vm_config$AccessKey)
  aws_secret_key <- Sys.getenv(vm_config$SecretKey)
  
  if (aws_access_key == "" || aws_secret_key == "") {
    stop(paste("AWS credentials not found in environment variables:", 
               vm_config$AccessKey, "or", vm_config$SecretKey))
  }
  
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
  
  log_msg <- paste0("Stopping existing instance: ", vm_config$InstanceId)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Stop the instance (not terminate)
  tryCatch({
    result <- ec2$stop_instances(list(InstanceIds = list(vm_config$InstanceId)))
    
    if (length(result$StoppingInstances) > 0) {
      log_msg <- paste0("Instance ", vm_config$InstanceId, " is stopping")
      faasr_log(log_msg)
      return(TRUE)
    } else {
      log_msg <- paste0("No instances were stopped for ID: ", vm_config$InstanceId)
      faasr_log(log_msg)
      return(FALSE)
    }
  }, error = function(e) {
    log_msg <- paste("Failed to stop instance", vm_config$InstanceId, ":", e$message)
    faasr_log(log_msg)
    cat(log_msg, "\n")
    return(FALSE)
  })
}

#' @name faasr_validate_vm_config_existing
#' @title Validate VM configuration for existing instance strategy
#' @param vm_config VM configuration
#' @export
faasr_validate_vm_config_existing <- function(vm_config) {
  
  required_fields <- c("Provider", "Region", "InstanceId")
  
  for (field in required_fields) {
    if (is.null(vm_config[[field]]) || vm_config[[field]] == "") {
      stop(paste("Required VM config field missing:", field))
    }
  }
  
  if (vm_config$Provider == "AWS") {
    aws_required <- c("AccessKey", "SecretKey", "InstanceId")
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
