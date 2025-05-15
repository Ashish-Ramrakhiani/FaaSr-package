#' @name faasr_vm_strategy
#' @title faasr_vm_strategy
#' @description 
#' Transforms a workflow to use VM-based execution for resource-intensive functions
#' @param faasr list with parsed workflow configuration
#' @param strategy string indicating VM strategy to use (default: "basic")
#' @return faasr transformed workflow configuration
#' @keywords internal

faasr_vm_strategy <- function(faasr, strategy="basic") {
  # Find resource-intensive functions
  ri_functions <- c()
  for (func_name in names(faasr$FunctionList)) {
    if (!is.null(faasr$FunctionList[[func_name]]$ResourceIntensive) && 
        ((is.logical(faasr$FunctionList[[func_name]]$ResourceIntensive) && 
          faasr$FunctionList[[func_name]]$ResourceIntensive == TRUE) || 
         is.list(faasr$FunctionList[[func_name]]$ResourceIntensive))) {
      ri_functions <- c(ri_functions, func_name)
    }
  }
  
  # If no resource-intensive functions, return original workflow
  if (length(ri_functions) == 0) {
    return(faasr)
  }
  
  cli_alert_info(paste0("Found ", length(ri_functions), " resource-intensive function(s): ", 
                        paste(ri_functions, collapse=", ")))
  
  # Find GitHub Actions server for VM management functions
  gh_server_name <- NULL
  for (server_name in names(faasr$ComputeServers)) {
    if (faasr$ComputeServers[[server_name]]$FaaSType == "GitHubActions") {
      gh_server_name <- server_name
      break
    }
  }
  
  if (is.null(gh_server_name)) {
    cli_alert_danger("No GitHub Actions server found for VM management")
    return(faasr)
  }
  
  # Check if VM server is defined
  vm_server_exists <- FALSE
  for (server_name in names(faasr$ComputeServers)) {
    if (faasr$ComputeServers[[server_name]]$FaaSType == "VM") {
      vm_server_exists <- TRUE
      break
    }
  }
  
  if (!vm_server_exists) {
    cli_alert_danger("No VM server defined in configuration")
    return(faasr)
  }
  
  # Add Deploy_VM function at the beginning of the workflow
  faasr$FunctionList$Deploy_VM <- list(
    FunctionName = "deploy_vm_faas",
    FaaSServer = gh_server_name,
    Arguments = list(
      resource_intensive_functions = ri_functions
    ),
    InvokeNext = faasr$FunctionInvoke  # Point to original entry point
  )
  
  # Find leaf functions (functions with no InvokeNext)
  leaf_functions <- c()
  for (func_name in names(faasr$FunctionList)) {
    if (func_name != "Deploy_VM" && 
        (is.null(faasr$FunctionList[[func_name]]$InvokeNext) || 
         length(faasr$FunctionList[[func_name]]$InvokeNext) == 0)) {
      leaf_functions <- c(leaf_functions, func_name)
    }
  }
  
  # Add Terminate_VM function at the end of the workflow
  faasr$FunctionList$Terminate_VM <- list(
    FunctionName = "terminate_vm_faas",
    FaaSServer = gh_server_name,
    Arguments = list()
  )
  
  # Update leaf functions to invoke Terminate_VM
  for (func_name in leaf_functions) {
    faasr$FunctionList[[func_name]]$InvokeNext <- "Terminate_VM"
    cli_alert_info(paste0("Updated function ", func_name, " to invoke Terminate_VM"))
  }
  
  # Update entry point to Deploy_VM
  original_entry <- faasr$FunctionInvoke
  faasr$FunctionInvoke <- "Deploy_VM"
  cli_alert_info(paste0("Changed workflow entry point from ", original_entry, " to Deploy_VM"))
  
  # Add functions to FunctionGitRepo if not already there
  if (!is.null(faasr$FunctionGitRepo)) {
    if (is.null(faasr$FunctionGitRepo$deploy_vm_faas)) {
      # Find a suitable repo to add the VM functions to
      if (length(faasr$FunctionGitRepo) > 0) {
        first_repo <- faasr$FunctionGitRepo[[1]]
        faasr$FunctionGitRepo$deploy_vm_faas <- first_repo
        faasr$FunctionGitRepo$terminate_vm_faas <- first_repo
        cli_alert_info(paste0("Added VM functions to FunctionGitRepo: ", first_repo))
      }
    }
  }
  
  # Ensure proper container is set for VM functions
  if (!is.null(faasr$ActionContainers)) {
    if (is.null(faasr$ActionContainers$Deploy_VM)) {
      # Find a container with AWS SDK support, or use the standard container
      aws_container <- "faasr/github-actions-aws:latest"
      if (aws_container %in% faasr$ActionContainers) {
        faasr$ActionContainers$Deploy_VM <- aws_container
        faasr$ActionContainers$Terminate_VM <- aws_container
      } else {
        # Use standard container and add dependencies
        std_container <- "faasr/github-actions-tidyverse:latest"
        faasr$ActionContainers$Deploy_VM <- std_container
        faasr$ActionContainers$Terminate_VM <- std_container
        
        # Add AWS SDK to dependencies
        if (is.null(faasr$FunctionCRANPackage)) {
          faasr$FunctionCRANPackage <- list()
        }
        faasr$FunctionCRANPackage$deploy_vm_faas <- c("paws.compute", "httr", "jsonlite", "base64enc")
        faasr$FunctionCRANPackage$terminate_vm_faas <- c("paws.compute", "httr", "jsonlite")
      }
      cli_alert_info("Set container and dependencies for VM functions")
    }
  }
  
  return(faasr)
}