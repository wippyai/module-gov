local json = require("json")
local time = require("time")
local uuid = require("uuid")
local security = require("security")
local logger = require("logger")

local log = logger:named("registry.client")

local client = {}

local CONST = {
    GOVERNANCE_PROCESS = "registry.governance",
    TOPICS = {
        COMMANDS = "registry.governance.command"
    },
    OPERATIONS = {
        APPLY_CHANGES = "apply_changes",
        APPLY_VERSION = "apply_version",
        UPLOAD = "upload",
        DOWNLOAD = "download",
        GET_STATE = "get_state"
    },
    PERMISSIONS = {
        WRITE = "registry.request.write",
        VERSION = "registry.request.version",
        SYNC = "registry.request.sync",
        READ = "registry.request.read"
    },
    DEFAULT_TIMEOUT = 600000
}

local function generate_id()
    local id, err = uuid.v4()
    if err then
        log:error("Failed to generate UUID", { error = err })
        error("Failed to generate UUID: " .. err)
    end
    return id
end

local function generate_channel_name()
    local id, err = uuid.v4()
    if err then
        log:error("Failed to generate UUID for channel name", { error = err })
        error("Failed to generate UUID for channel name: " .. err)
    end
    return "registry.response." .. id
end

local function get_user_id()
    local actor = security.actor()
    if actor then
        return actor:id()
    end
    return nil
end

local function check_permission(permission, resource)
    local can, err = security.can(permission, resource)

    if err then
        log:warn("Security check error", { permission = permission, error = err })
        return false, "Security check error: " .. err
    end
    if not can then
        log:warn("Permission denied", { permission = permission, resource = resource })
        return false, "Permission denied: " .. permission .. (resource and (" for " .. resource) or "")
    end
    return true
end

local function extract_changeset(changeset)
    if type(changeset) == "table" and changeset[1] and changeset[1].kind then
        return changeset
    end

    if type(changeset) == "userdata" and type(changeset.ops) == "function" then
        return changeset:ops()
    end

    return nil, "Invalid changeset format"
end

local function send_and_wait(message, timeout_ms)
    timeout_ms = timeout_ms or CONST.DEFAULT_TIMEOUT

    local response_channel_name = generate_channel_name()

    message.respond_to = response_channel_name

    local response_channel = process.listen(response_channel_name)

    local ok = process.send(CONST.GOVERNANCE_PROCESS, CONST.TOPICS.COMMANDS, message)
    if not ok then
        log:error("Failed to send message", { recipient = CONST.GOVERNANCE_PROCESS })
        return nil, "Failed to send message to governance process"
    end

    log:debug("Sent message, waiting for response", {
        operation = message.operation,
        timeout_ms = timeout_ms
    })

    local timeout = time.after(timeout_ms)

    local result = channel.select({
        response_channel:case_receive(),
        timeout:case_receive()
    })

    if result.channel == timeout then
        log:error("Operation timed out", { timeout_seconds = timeout_ms / 1000 })
        return nil, "Operation timed out after " .. (timeout_ms / 1000) .. " seconds"
    end

    local response = result.value

    if not response.request_id or response.request_id ~= message.id then
        log:error("Received response for different request", {
            expected = message.id,
            received = response.request_id
        })
        return nil, "Received response for a different request"
    end

    log:debug("Received response", {
        success = response.success,
        operation = message.operation
    })

    return response
end

function client.get_state(options, timeout_ms)
    local ok, err = check_permission(CONST.PERMISSIONS.READ, "state")
    if not ok then
        return nil, err
    end

    local user_id = get_user_id()

    local message = {
        id = generate_id(),
        operation = CONST.OPERATIONS.GET_STATE,
        options = options or {},
        user_id = user_id,
        timestamp = time.now():unix()
    }

    log:debug("Requesting registry system state", { user_id = user_id })

    local response, err = send_and_wait(message, timeout_ms)
    if not response then
        return nil, err
    end

    if response.success then
        log:debug("Got registry system state")
        return response.state, nil
    else
        log:error("Failed to get registry system state", {
            error = response.message
        })
        return nil, response.message or "Unknown error"
    end
end

function client.request_changes(changeset, options, timeout_ms)
    local ok, err = check_permission(CONST.PERMISSIONS.WRITE, "changeset")
    if not ok then
        return nil, err
    end

    local processed_changeset, err = extract_changeset(changeset)
    if not processed_changeset then
        log:error("Failed to extract changeset", { error = err })
        return nil, err
    end

    local user_id = get_user_id()

    local message = {
        id = generate_id(),
        operation = CONST.OPERATIONS.APPLY_CHANGES,
        changeset = processed_changeset,
        options = options or {},
        user_id = user_id,
        timestamp = time.now():unix(),
    }

    log:info("Requesting registry changes", {
        changeset_count = #processed_changeset,
        user_id = user_id
    })

    local response, err = send_and_wait(message, timeout_ms)
    if not response then
        return nil, err
    end

    if response.success then
        log:info("Changes applied successfully", {
            version = response.version,
            has_details = response.details ~= nil,
            has_changeset = response.changeset ~= nil
        })

        return {
            version = response.version,
            message = response.message,
            details = response.details,
            changeset = response.changeset
        }, nil
    else
        log:error("Failed to apply changes", {
            error = response.message
        })
        return nil, response.message or "Unknown error"
    end
end

function client.request_version(version_id, options, timeout_ms)
    local ok, err = check_permission(CONST.PERMISSIONS.VERSION, "version")
    if not ok then
        return nil, err
    end

    if type(version_id) ~= "string" then
        version_id = tostring(version_id)
    end

    local user_id = get_user_id()

    local message = {
        id = generate_id(),
        operation = CONST.OPERATIONS.APPLY_VERSION,
        version_id = version_id,
        options = options or {},
        user_id = user_id,
        timestamp = time.now():unix()
    }

    log:info("Requesting version application", {
        version_id = version_id,
        user_id = user_id
    })

    local response, err = send_and_wait(message, timeout_ms)
    if not response then
        return nil, err
    end

    if response.success then
        log:info("Version applied successfully", {
            version_id = version_id,
            has_details = response.details ~= nil,
            has_changeset = response.changeset ~= nil
        })

        return {
            version = version_id,
            message = response.message,
            details = response.details,
            changeset = response.changeset
        }, nil
    else
        log:error("Failed to apply version", {
            version_id = version_id,
            error = response.message
        })
        return nil, response.message or "Unknown error"
    end
end

function client.request_download(options, timeout_ms)
    local ok, err = check_permission(CONST.PERMISSIONS.SYNC, "download")
    if not ok then
        return nil, err
    end

    local user_id = get_user_id()

    local message = {
        id = generate_id(),
        operation = CONST.OPERATIONS.DOWNLOAD,
        options = options or {},
        user_id = user_id,
        timestamp = time.now():unix()
    }

    log:info("Requesting download", { user_id = user_id })

    local response, err = send_and_wait(message, timeout_ms)
    if not response then
        return nil, err
    end

    if response.success then
        log:info("Download completed successfully", {
            stats = response.stats,
            has_details = response.details ~= nil,
            has_changeset = response.changeset ~= nil
        })

        return {
            version = response.version,
            stats = response.stats,
            message = response.message,
            details = response.details,
            changeset = response.changeset
        }, nil
    else
        log:error("Download failed", {
            error = response.message
        })
        return nil, response.message or "Unknown error"
    end
end

function client.request_upload(options, timeout_ms)
    local ok, err = check_permission(CONST.PERMISSIONS.SYNC, "upload")
    if not ok then
        return nil, err
    end

    local user_id = get_user_id()

    local message = {
        id = generate_id(),
        operation = CONST.OPERATIONS.UPLOAD,
        options = options or {},
        user_id = user_id,
        timestamp = time.now():unix()
    }

    log:info("Requesting upload", { user_id = user_id })

    local response, err = send_and_wait(message, timeout_ms)
    if not response then
        return nil, err
    end

    if response.success then
        log:info("Upload completed successfully", {
            stats = response.stats,
            has_details = response.details ~= nil,
            has_changeset = response.changeset ~= nil
        })

        return {
            version = response.version,
            stats = response.stats,
            message = response.message,
            details = response.details,
            changeset = response.changeset
        }, nil
    else
        log:error("Upload failed", {
            error = response.message
        })
        return nil, response.message or "Unknown error"
    end
end

return client