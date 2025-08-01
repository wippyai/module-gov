local uploader = require("uploader")
local json = require("json")
local logger = require("logger")

-- Create a named logger for this process
local log = logger:named("wippy.gov.sync.uploader")

-- Main run function
local function run(args)
    log:info("Starting uploader process")

    -- Validate arguments
    if not args then
        log:error("No arguments provided")
        return {
            success = false,
            message = "No arguments provided",
            error = "Missing required arguments"
        }
    end

    -- Check if we need to check for changes or actually upload
    if args.check_only then
        log:info("Checking for changes only")
        local result = uploader.has_changes(args.options or {})
        return result
    else
        log:info("Performing upload operation - building changeset")
        local result = uploader.upload(args.options or {})

        -- Return the changeset for the governance process to handle
        if result.success then
            log:info("Successfully built changeset from filesystem")

            return {
                success = true,
                message = "Successfully built changeset from filesystem",
                changeset = result.changeset,
                count = result.count,
                stats = result.stats
            }
        else
            -- If error, return the result as is
            return result
        end
    end
end

-- Export the run function
return { run = run }