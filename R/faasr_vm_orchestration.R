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
  
  # FIXED: Check if function has predecessors (incoming edges)
  has_predecessors <- FALSE
  for (func_name in names(workflow)) {
    func_config <- workflow[[func_name]]
    invoke_next <- func_config$InvokeNext
    
    if (!is.null(invoke_next)) {
      # Handle both single string and array of strings
      if (is.character(invoke_next)) {
        if (current_function %in% invoke_next) {
          has_predecessors <- TRUE
          break
        }
      }
    }
  }
  
  # FIXED: Check if function has successors (outgoing edges)
  current_config <- workflow[[current_function]]
  has_successors <- !is.null(current_config$InvokeNext) && length(current_config$InvokeNext) > 0
  
  # Determine function type based on predecessors and successors
  if (!has_predecessors && !has_successors) {
    function_type <- "single"  # Only one function in workflow
    is_first <- TRUE
    is_last <- TRUE
  } else if (!has_predecessors && has_successors) {
    function_type <- "first"   # Entry point function
    is_first <- TRUE
    is_last <- FALSE
  } else if (has_predecessors && !has_successors) {
    function_type <- "last"    # Terminal function
    is_first <- FALSE
    is_last <- TRUE
  } else {
    function_type <- "middle"  # Middle function
    is_first <- FALSE
    is_last <- FALSE
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
#' @title Wait for VM to be ready - improved with status verification
#' @param .faasr FaaSr configuration list
#' @param vm_details VM details from start operation
#' @export
faasr_vm_wait_ready_simplified <- function(.faasr, vm_details) {
  
  max_wait_time <- 300  # 5 minutes total
  check_interval <- 20  # Check every 20 seconds
  start_time <- Sys.time()
  
  log_msg <- "Waiting for existing VM to be ready..."
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Get VM config for status checking
  vm_config <- .faasr$VMConfig
  
  while (TRUE) {
    elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    
    # Timeout check
    if (elapsed_time > max_wait_time) {
      log_msg <- "VM wait timeout reached - proceeding (runner may not be ready)"
      faasr_log(log_msg)
      cat(log_msg, "\n")
      break
    }
    
    # Check actual VM status
    tryCatch({
      vm_status <- faasr_check_vm_status(vm_config)
      
      if (vm_status$instance_running && vm_status$status_checks_passed) {
        # VM is running and healthy, now wait a bit more for GitHub runner service
        log_msg <- paste0("VM is running and healthy after ", round(elapsed_time), " seconds")
        faasr_log(log_msg)
        cat(log_msg, "\n")
        
        # Additional wait for GitHub runner service to register (if we haven't waited long enough)
        if (elapsed_time < 90) {
          additional_wait <- 90 - elapsed_time
          log_msg <- paste0("Waiting additional ", round(additional_wait), " seconds for GitHub runner service...")
          faasr_log(log_msg)
          cat(log_msg, "\n")
          Sys.sleep(additional_wait)
        }
        
        log_msg <- "VM and GitHub runner service should be ready"
        faasr_log(log_msg)
        cat(log_msg, "\n")
        break
      } else {
        # VM not ready yet
        status_msg <- paste0("VM Status - Running: ", vm_status$instance_running, 
                             ", Status Checks: ", vm_status$status_checks_passed)
        cat(".", status_msg, "\n")
      }
      
    }, error = function(e) {
      # If status check fails, continue waiting
      cat(".", "Status check failed:", e$message, "\n")
    })
    
    Sys.sleep(check_interval)
  }
}

#' @name faasr_vm_wait_stopped
#' @title Wait for VM to be fully stopped
#' @param vm_config VM configuration including InstanceId
#' @export
faasr_vm_wait_stopped <- function(vm_config) {
  
  max_wait_time <- 180  # 3 minutes for stopping
  check_interval <- 15  # Check every 15 seconds
  start_time <- Sys.time()
  
  log_msg <- "Waiting for VM to be fully stopped..."
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  while (TRUE) {
    elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    
    # Timeout check
    if (elapsed_time > max_wait_time) {
      log_msg <- "VM stop wait timeout reached - proceeding anyway"
      faasr_log(log_msg)
      cat(log_msg, "\n")
      break
    }
    
    # Check actual VM status
    tryCatch({
      vm_status <- faasr_check_vm_status(vm_config)
      
      if (!vm_status$instance_running) {
        log_msg <- paste0("VM successfully stopped after ", round(elapsed_time), " seconds")
        faasr_log(log_msg)
        cat(log_msg, "\n")
        break
      } else {
        cat(".", "VM still running, waiting for stop...\n")
      }
      
    }, error = function(e) {
      # If status check fails, continue waiting
      cat(".", "Status check failed:", e$message, "\n")
    })
    
    Sys.sleep(check_interval)
  }
}

#' @name faasr_check_vm_status
#' @title Check VM instance status using AWS API
#' @param vm_config VM configuration including credentials and instance ID
#' @return List with instance_running and status_checks_passed flags
#' @export
faasr_check_vm_status <- function(vm_config) {
  
  # Get AWS credentials (should already be replaced by faasr_replace_values)
  aws_access_key <- vm_config$AccessKey
  aws_secret_key <- vm_config$SecretKey
  
  if (is.null(aws_access_key) || is.null(aws_secret_key) || 
      aws_access_key == "" || aws_secret_key == "" ||
      aws_access_key == "VMCONFIG_ACCESS_KEY" || aws_secret_key == "VMCONFIG_SECRET_KEY") {
    stop("AWS credentials not properly replaced - cannot check VM status")
  }
  
  # Create EC2 client using working S3-style creds wrapper format
  ec2 <- paws.compute::ec2(
    config = list(
      credentials = list(
        creds = list(
          access_key_id = aws_access_key,
          secret_access_key = aws_secret_key
        )
      ),
      region = vm_config$Region
    )
  )
  
  # Get instance status
  result <- ec2$describe_instances(InstanceIds = list(vm_config$InstanceId))
  
  if (length(result$Reservations) == 0 || length(result$Reservations[[1]]$Instances) == 0) {
    stop(paste("Instance not found:", vm_config$InstanceId))
  }
  
  instance <- result$Reservations[[1]]$Instances[[1]]
  instance_state <- instance$State$Name
  
  # Check if instance is running
  instance_running <- (instance_state == "running")
  
  # Check system status (only if running)
  status_checks_passed <- FALSE
  if (instance_running) {
    tryCatch({
      # Get instance status checks
      status_result <- ec2$describe_instance_status(InstanceIds = list(vm_config$InstanceId))
      
      if (length(status_result$InstanceStatuses) > 0) {
        status_info <- status_result$InstanceStatuses[[1]]
        instance_status <- status_info$InstanceStatus$Status
        system_status <- status_info$SystemStatus$Status
        
        # Both instance and system status should be "ok"
        status_checks_passed <- (instance_status == "ok" && system_status == "ok")
      } else {
        # No status information available yet (instance might be starting)
        status_checks_passed <- FALSE
      }
    }, error = function(e) {
      # If status check API fails, assume not ready
      status_checks_passed <- FALSE
    })
  }
  
  return(list(
    instance_running = instance_running,
    status_checks_passed = status_checks_passed,
    instance_state = instance_state
  ))
}

#' @name faasr_aws_start_existing_vm
#' @title Start existing AWS EC2 instance following FaaSr credential pattern
#' @param vm_config VM configuration including InstanceId
#' @param faasr FaaSr configuration list
#' @importFrom paws.compute ec2
#' @export
faasr_aws_start_existing_vm <- function(vm_config, faasr = NULL) {
  
  # Validate required fields for existing instance
  if (is.null(vm_config$InstanceId) || vm_config$InstanceId == "") {
    stop("InstanceId is required in VMConfig for existing instance strategy")
  }
  
  # FaaSr credential pattern: credentials are already replaced by faasr_replace_values()
  aws_access_key <- vm_config$AccessKey
  aws_secret_key <- vm_config$SecretKey
  
  if (is.null(aws_access_key) || is.null(aws_secret_key) || 
      aws_access_key == "" || aws_secret_key == "" ||
      aws_access_key == "VMCONFIG_ACCESS_KEY" || aws_secret_key == "VMCONFIG_SECRET_KEY") {
    stop("AWS credentials not properly replaced - check SECRET_PAYLOAD and VMConfig setup")
  }
  
  log_msg <- "Using AWS credentials from FaaSr credential replacement"
  faasr_log(log_msg)
  
  # Create EC2 client using WORKING S3-style creds wrapper format
  ec2 <- paws.compute::ec2(
    config = list(
      credentials = list(
        creds = list(
          access_key_id = aws_access_key,
          secret_access_key = aws_secret_key
        )
      ),
      region = vm_config$Region
    )
  )
  
  # Check current instance state before attempting start
  tryCatch({
    current_status <- faasr_check_vm_status(vm_config)
    if (current_status$instance_running) {
      log_msg <- paste0("Instance ", vm_config$InstanceId, " is already running")
      faasr_log(log_msg)
      cat(log_msg, "\n")
      return(list(
        InstanceId = vm_config$InstanceId,
        State = "running",
        Provider = "AWS"
      ))
    }
  }, error = function(e) {
    log_msg <- paste0("Could not check current instance state: ", e$message)
    faasr_log(log_msg)
    cat(log_msg, "\n")
  })
  
  log_msg <- paste0("Starting existing instance: ", vm_config$InstanceId)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Start the existing instance
  tryCatch({
    result <- ec2$start_instances(InstanceIds = list(vm_config$InstanceId))
    
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
#' @title Stop existing AWS EC2 instance following FaaSr credential pattern
#' @param vm_config VM configuration including InstanceId
#' @export
faasr_aws_stop_existing_vm <- function(vm_config) {
  
  # FaaSr credential pattern: credentials are already replaced by faasr_replace_values()
  aws_access_key <- vm_config$AccessKey
  aws_secret_key <- vm_config$SecretKey
  
  if (is.null(aws_access_key) || is.null(aws_secret_key) || 
      aws_access_key == "" || aws_secret_key == "" ||
      aws_access_key == "VMCONFIG_ACCESS_KEY" || aws_secret_key == "VMCONFIG_SECRET_KEY") {
    stop("AWS credentials not properly replaced - check SECRET_PAYLOAD and VMConfig setup")
  }
  
  # Create EC2 client using WORKING S3-style creds wrapper format
  ec2 <- paws.compute::ec2(
    config = list(
      credentials = list(
        creds = list(
          access_key_id = aws_access_key,
          secret_access_key = aws_secret_key
        )
      ),
      region = vm_config$Region
    )
  )
  
  # Check current instance state before attempting stop
  tryCatch({
    current_status <- faasr_check_vm_status(vm_config)
    if (!current_status$instance_running) {
      log_msg <- paste0("Instance ", vm_config$InstanceId, " is already stopped")
      faasr_log(log_msg)
      cat(log_msg, "\n")
      return(TRUE)
    }
  }, error = function(e) {
    log_msg <- paste0("Could not check current instance state: ", e$message)
    faasr_log(log_msg)
    cat(log_msg, "\n")
  })
  
  log_msg <- paste0("Stopping existing instance: ", vm_config$InstanceId)
  faasr_log(log_msg)
  cat(log_msg, "\n")
  
  # Stop the instance (not terminate)
  tryCatch({
    result <- ec2$stop_instances(InstanceIds = list(vm_config$InstanceId))
    
    if (length(result$StoppingInstances) > 0) {
      log_msg <- paste0("Instance ", vm_config$InstanceId, " is stopping")
      faasr_log(log_msg)
      
      # Wait for the instance to be fully stopped
      faasr_vm_wait_stopped(vm_config)
      
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
