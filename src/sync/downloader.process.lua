local downloader = require("downloader")
local logger = require("logger")

-- Create a named logger for this process
local log = logger:named("wippy.gov.sync.downloader")

-- Main run function
local function run(args)
    log:info("Starting downloader process")

    -- Validate arguments
    if not args then
        log:error("No arguments provided")
        return {
            success = false,
            message = "No arguments provided",
            error = "Missing required arguments"
        }
    end

    -- Perform download operation
    log:info("Performing download operation")
    local result = downloader.download(args.options or {})

    return result
end

-- Export the run function
return { run = run }