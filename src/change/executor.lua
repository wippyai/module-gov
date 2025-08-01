local registry = require("registry")
local json = require("json")
local time = require("time")
local logger = require("logger")

-- Create a named logger for this worker
local log = logger:named("wippy.gov.change.exec")

-- Constants for operations and response formats
local CONST = {
    OPERATIONS = {
        APPLY_CHANGES = "apply_changes",
        APPLY_VERSION = "apply_version"
    },
    OP = {
        CREATE = "entry.create",
        UPDATE = "entry.update",
        DELETE = "entry.delete"
    }
}

-- Apply specific registry changeset
local function apply_changes(changeset, options, request_id, user_id)
    log:info("Applying registry changes", {
        changeset_count = #changeset,
        options = options,
        request_id = request_id,
        user_id = user_id
    })

    -- Create a snapshot for applying changes
    local snapshot, err = registry.snapshot()
    if not snapshot then
        log:error("Failed to create registry snapshot", {
            error = err,
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Failed to create registry snapshot",
            error = err or "Unknown error",
            request_id = request_id,
            user_id = user_id
        }
    end

    -- Get changes object for the snapshot
    local changes = snapshot:changes()
    if not changes then
        log:error("Failed to get changes object", {
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Failed to get changes object from snapshot",
            error = "Changes object not available",
            request_id = request_id,
            user_id = user_id
        }
    end

    -- Apply changeset to the changes object
    for _, op in ipairs(changeset) do
        local kind = op.kind
        local entry = op.entry

        if kind == CONST.OP.CREATE then
            log:debug("Creating entry", {
                id = entry.id or (entry.namespace and entry.name and (entry.namespace .. ":" .. entry.name)),
                request_id = request_id,
                user_id = user_id
            })
            changes:create(entry)
        elseif kind == CONST.OP.UPDATE then
            log:debug("Updating entry", {
                id = entry.id or (entry.namespace and entry.name and (entry.namespace .. ":" .. entry.name)),
                request_id = request_id,
                user_id = user_id
            })
            changes:update(entry)
        elseif kind == CONST.OP.DELETE then
            local id = entry.id or (entry.namespace and entry.name and (entry.namespace .. ":" .. entry.name))
            log:debug("Deleting entry", {
                id = id,
                request_id = request_id,
                user_id = user_id
            })
            changes:delete(id)
        end
    end

    -- Apply the changes to create a new registry version
    local version, err = changes:apply()
    if not version then
        -- Handle "no changes to apply" case (might happen if entries already match)
        if err and tostring(err):find("no changes to apply") then
            log:info("No changes needed to be applied", {
                request_id = request_id,
                user_id = user_id
            })
            return {
                success = true,
                message = "No changes needed to be applied",
                request_id = request_id,
                user_id = user_id
            }
        else
            -- Actual error case
            log:error("Failed to apply changes", {
                error = err,
                request_id = request_id,
                user_id = user_id
            })
            return {
                success = false,
                message = "Failed to apply changes to registry",
                error = err or "Unknown error",
                request_id = request_id,
                user_id = user_id
            }
        end
    end

    log:info("Successfully applied changes", {
        version = version:id(),
        request_id = request_id,
        user_id = user_id
    })

    -- Return success with the resulting version
    return {
        success = true,
        message = "Successfully applied changes to registry",
        version = version:id(),
        request_id = request_id,
        user_id = user_id
    }
end

-- Apply a specific registry version
local function apply_version(version_id, options, request_id, user_id)
    log:info("Applying registry version", {
        version_id = version_id,
        options = options,
        request_id = request_id,
        user_id = user_id
    })

    -- Get the specified version using the history object
    local history = registry.history()
    local version, err = history:get_version(version_id)
    if not version then
        log:error("Failed to get registry version", {
            version_id = version_id,
            error = err,
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Failed to get registry version: " .. version_id,
            error = err or "Unknown error",
            request_id = request_id,
            user_id = user_id
        }
    end

    -- Apply the version
    local success, err = registry.apply_version(version)
    if not success then
        log:error("Failed to apply registry version", {
            version_id = version_id,
            error = err,
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Failed to apply registry version: " .. version_id,
            error = err or "Unknown error",
            request_id = request_id,
            user_id = user_id
        }
    end

    log:info("Successfully applied registry version", {
        version_id = version_id,
        request_id = request_id,
        user_id = user_id
    })

    -- Return success with the version
    return {
        success = true,
        message = "Successfully applied registry version: " .. version_id,
        version = version_id,
        request_id = request_id,
        user_id = user_id
    }
end

-- Main run function that executes the appropriate operation
local function run(args)
    log:info("Starting registry change executor process")

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

    if not request_id then
        log:warn("No request_id provided")
        request_id = "unknown"
    end

    -- Check for pre-processor result, which might contain updated changeset/version
    local pre_result = args.pre_result

    -- Determine and execute the appropriate operation
    if args.changeset then
        return apply_changes(args.changeset, args.options or {}, request_id, user_id)
    elseif args.version_id then
        return apply_version(args.version_id, args.options or {}, request_id, user_id)
    else
        log:error("Invalid arguments - requires changeset or version_id", {
            request_id = request_id,
            user_id = user_id
        })
        return {
            success = false,
            message = "Invalid arguments provided",
            error = "Must provide either changeset or version_id",
            request_id = request_id,
            user_id = user_id
        }
    end
end

-- Export the run function
return { run = run }