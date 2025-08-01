local registry = require("registry")
local json = require("json")
local time = require("time")
local logger = require("logger")
local funcs = require("funcs")

-- Create a named logger for this process
local log = logger:named("wippy.gov.change")

-- Constants for operations and function names
local CONST = {
    OPERATIONS = {
        APPLY_CHANGES = "apply_changes",
        APPLY_VERSION = "apply_version"
    },
    FUNCTIONS = {
        PRE_PROCESSOR = "wippy.gov.change:pre_processor",
        EXECUTOR = "wippy.gov.change:executor",
        POST_PROCESSOR = "wippy.gov.change:post_processor"
    }
}

-- Main run function that coordinates the sub-processes
local function run(args)
    log:info("Starting registry change process")

    -- Validate arguments
    if not args then
        log:error("No arguments provided")
        return {
            success = false,
            message = "No arguments provided",
            error = "Missing required arguments"
        }
    end

    local request_id = args.request_id
    if not request_id then
        log:error("No request_id provided")
        return {
            success = false,
            message = "No request_id provided",
            error = "Request ID is required"
        }
    end

    local user_id = args.user_id

    log:info("Processing request", {
        request_id = request_id,
        user_id = user_id
    })

    -- Create function executor
    local executor = funcs.new()

    -- Create context with request ID and user ID for all function calls
    local ctx_executor = executor:with_context({
        request_id = request_id,
        user_id = user_id
    })

    -- Step 1: Pre-processing phase
    log:debug("Starting pre-processing phase", {
        request_id = request_id,
        user_id = user_id
    })
    local pre_result, err = ctx_executor:call(CONST.FUNCTIONS.PRE_PROCESSOR, {
        request_id = request_id,
        user_id = user_id,
        changeset = args.changeset,
        version_id = args.version_id,
        options = args.options or {}
    })

    if err then
        log:error("Pre-processor error", {
            error = err,
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Pre-processing failed",
            error = err,
            request_id = request_id
        }
    end

    if not pre_result.success then
        log:error("Pre-processor reported failure", {
            message = pre_result.message,
            request_id = request_id,
            user_id = user_id
        })
        return pre_result
    end

    -- Step 2: Execution phase
    log:debug("Starting execution phase", {
        request_id = request_id,
        user_id = user_id
    })
    local exec_result, err = ctx_executor:call(CONST.FUNCTIONS.EXECUTOR, {
        request_id = request_id,
        user_id = user_id,
        changeset = pre_result.changeset or args.changeset,
        version_id = pre_result.version_id or args.version_id,
        options = pre_result.options or args.options or {}
    })

    if err then
        log:error("Executor error", {
            error = err,
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Execution failed",
            error = err,
            request_id = request_id,
            details = pre_result.details
        }
    end

    if not exec_result.success then
        log:error("Executor reported failure", {
            message = exec_result.message,
            error = exec_result.error,
            request_id = request_id,
            user_id = user_id
        })
        -- Add details from pre_result to exec_result
        exec_result.details = pre_result.details
        -- Ensure options are preserved in the result
        exec_result.options = pre_result.options or args.options
        return exec_result
    end

    -- Retain the changeset
    exec_result.changeset = exec_result.changeset or pre_result.changeset or args.changeset

    -- Ensure options are preserved in the result (may contain token/ticket)
    exec_result.options = exec_result.options or pre_result.options or args.options

    -- Ensure user_id is preserved in the result
    exec_result.user_id = user_id

    -- Add details from pre_result to exec_result
    exec_result.details = pre_result.details

    -- Step 3: Post-processing phase
    log:debug("Running post-processors", {
        request_id = request_id,
        user_id = user_id
    })
    local post_result, err = ctx_executor:call(CONST.FUNCTIONS.POST_PROCESSOR, {
        request_id = request_id,
        user_id = user_id,
        execution_result = exec_result,
        options = exec_result.options -- Explicitly pass options for authentication
    })

    -- Even if post-processing fails, we return the execution result
    if err then
        log:warn("Post-processor error, continuing with execution result", {
            error = err,
            request_id = request_id,
            user_id = user_id
        })
    end

    log:info("Completed registry change process", {
        request_id = request_id,
        user_id = user_id
    })

    return exec_result
end

-- Export the run function and process constants
return {
    run = run,
    process = CONST.FUNCTIONS
}