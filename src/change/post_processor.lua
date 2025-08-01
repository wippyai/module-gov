local registry = require("registry")
local json = require("json")
local time = require("time")
local logger = require("logger")
local listener_registry = require("listener_registry")

-- Create a named logger for this worker
local log = logger:named("wippy.gov.change.post")

-- Main run function for post-processing
local function run(args)
    log:info("Starting post-processing phase")

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
    local user_id = args.user_id

    -- Get the execution result
    local execution_result = args.execution_result
    if not execution_result then
        log:error("No execution result provided", {
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "No execution result provided",
            error = "Missing execution result",
            request_id = request_id,
            user_id = user_id
        }
    end

    -- If execution failed, just return the execution result without post-processing
    if not execution_result.success then
        log:warn("Execution failed, skipping post-processing", {
            error = execution_result.error,
            request_id = request_id,
            user_id = user_id
        })
        -- Ensure user_id is preserved in the result
        execution_result.user_id = user_id
        return execution_result
    end

    log:info("Processing successful execution result", {
        request_id = request_id,
        user_id = user_id,
        has_version = execution_result.version ~= nil
    })

    -- If operations were processed, notify listeners
    if execution_result.changeset then
       listener_registry.run_listeners(
            execution_result.changeset,
            execution_result,
            request_id,
            user_id
        )
    end

    -- Ensure user_id is preserved in the result
    execution_result.user_id = user_id

    return nil
end

-- Export the run function
return { run = run }