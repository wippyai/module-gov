local registry = require("registry")
local json = require("json")
local fs_module = require("fs")
local yaml = require("yaml")
local env = require("env")

-- Main module
local downloader = {}

---------------------------
-- Constants
---------------------------

-- Operation types
downloader.OP = {
    CREATE = "entry.create",
    UPDATE = "entry.update",
    DELETE = "entry.delete"
}

-- Debug log levels
downloader.LOG_LEVEL = {
    ERROR = 1,
    WARN = 2,
    INFO = 3,
    DEBUG = 4,
    TRACE = 5
}

-- Current log level (change this to control verbosity)
local current_log_level = downloader.LOG_LEVEL.INFO

-- Define file mapping configurations by kind
downloader.KIND_CONFIG = {
    ["function.lua"] = {
        source_field = "source",
        extension = ".lua"
    },
    ["library.lua"] = {
        source_field = "source",
        extension = ".lua"
    },
    ["process.lua"] = {
        source_field = "source",
        extension = ".lua"
    },
    ["btea.app.lua"] = {
        source_field = "source",
        extension = ".lua"
    },
    ["template.jet"] = {
        source_field = "source",
        extension = ".jet"
    },
    ["registry.entry"] = {
        -- Type-specific configurations
        types = {
            ["view.page"] = {
                source_field = "source",
                extension = ".html"
            },
            ["module.spec"] = {
                source_field = "source",
                extension = ".md"
            },
            ["agent.gen1"] = {
                source_field = "source",
                extension = ".yml"
            }
        }
    }
}

-- Define field order priority for YAML output
downloader.FIELD_ORDER = {
    -- Top-level important fields
    "version",
    "namespace",
    -- Entry level important fields
    "name",
    "kind",
    "contract",
    -- Shared fields
    "meta",
    -- Meta fields
    "type",
    "title",
    "comment",
    "group",
    "tags",
    "icon",
    "description",
    "order",
    "content_type",
    -- Common fields for agents
    "prompt",
    "model",
    "temperature",
    "max_tokens",
    "tools",
    "memory",
    "delegate",
    -- Common fields for functions
    "source",
    "modules",
    "imports",
    "method",
    -- Dependencies
    "depends_on",
    "router",
    -- Common resource fields
    "set",
    "resources",
    -- Functional fields
    "entries",
    -- Everything else will be sorted alphabetically
}

---------------------------
-- Logging Functions
---------------------------

-- Set the current log level
function downloader.set_log_level(level)
    if type(level) == "number" and level >= downloader.LOG_LEVEL.ERROR and level <= downloader.LOG_LEVEL.TRACE then
        current_log_level = level
    end
end

-- Log messages based on level
local function log(level, message, data)
    if level <= current_log_level then
        local prefix = ""
        if level == downloader.LOG_LEVEL.ERROR then
            prefix = "[ERROR] "
        elseif level == downloader.LOG_LEVEL.WARN then
            prefix = "[WARN] "
        elseif level == downloader.LOG_LEVEL.INFO then
            prefix = "[INFO] "
        elseif level == downloader.LOG_LEVEL.DEBUG then
            prefix = "[DEBUG] "
        elseif level == downloader.LOG_LEVEL.TRACE then
            prefix = "[TRACE] "
        end

        local output = prefix .. message

        -- Add data if provided
        if data ~= nil then
            if type(data) == "table" then
                local success, json_data = pcall(json.encode, data)
                if success then
                    output = output .. ": " .. json_data
                else
                    output = output .. ": <table data>"
                end
            else
                output = output .. ": " .. tostring(data)
            end
        end

        print(output)
    end
end

local function error_log(message, data) log(downloader.LOG_LEVEL.ERROR, message, data) end
local function warn_log(message, data) log(downloader.LOG_LEVEL.WARN, message, data) end
local function info_log(message, data) log(downloader.LOG_LEVEL.INFO, message, data) end
local function debug_log(message, data) log(downloader.LOG_LEVEL.DEBUG, message, data) end
local function trace_log(message, data) log(downloader.LOG_LEVEL.TRACE, message, data) end

---------------------------
-- Helper Functions
---------------------------

-- Get the target directory from environment or options
local function get_target_directory(options)
    -- Return from options if provided
    if options and options.directory then
        return options.directory
    end

    -- Try to get from environment variable
    local app_src, err = env.get("APP_SRC")
    if not err and app_src and #app_src > 0 then
        return app_src
    end

    -- If no environment variable found, return error
    return nil, "Target directory not provided in options and APP_SRC environment variable not set"
end

-- Get the filesystem identifier from environment or options
local function get_filesystem_id(options)
    -- Return from options if provided
    if options and options.filesystem then
        return options.filesystem
    end

    -- Try to get from environment variable
    local app_fs, err = env.get("APP_FS")
    if not err and app_fs and #app_fs > 0 then
        return app_fs
    end

    -- If no environment variable found, return error
    return nil, "Filesystem ID not provided in options and APP_FS environment variable not set"
end

-- Helper function to ensure directory exists
local function ensure_directory(fs, base_dir, path)
    local current_path = base_dir
    for part in string.gmatch(string.gsub(path, "%.", "/"), "([^/]+)") do
        current_path = current_path .. "/" .. part
        if not fs:exists(current_path) then
            debug_log("Creating directory", current_path)
            fs:mkdir(current_path)
        end
    end
    return current_path
end

-- Extract filename from file:// URL
local function extract_filename(file_url)
    if not file_url or type(file_url) ~= "string" then
        return nil
    end

    local filename = file_url:match("^file://(.+)$")
    return filename
end

-- Compare file content to avoid unnecessary writes
local function should_write_file(fs, file_path, content)
    if not fs:exists(file_path) then
        trace_log("File doesn't exist, writing new file", file_path)
        return true
    end

    local current_content = fs:readfile(file_path)
    if current_content ~= content then
        trace_log("File content differs, updating file", file_path)
        return true
    end

    trace_log("File content unchanged, skipping write", file_path)
    return false
end

-- Delete a source file if it exists and is referenced with file://
local function delete_source_file(fs, directory, namespace, entry, kind_config)
    if not entry or not kind_config then
        return false
    end

    -- Determine the source field
    local source_field = nil
    local config = nil

    -- Check if we have a direct kind config or need to check meta.type
    if kind_config.types and entry.meta and entry.meta.type and kind_config.types[entry.meta.type] then
        -- Use type-specific config
        config = kind_config.types[entry.meta.type]
    else
        -- Use direct kind config
        config = kind_config
    end

    if config then
        source_field = config.source_field
    end

    -- If no source field determined, nothing to delete
    if not source_field or not entry[source_field] then
        return false
    end

    -- Check if it's a file:// reference
    local filename = extract_filename(entry[source_field])
    if not filename then
        return false
    end

    -- Prepare directory path
    local dir_path = ensure_directory(fs, directory, namespace)

    -- Full file path
    local file_path = dir_path .. "/" .. filename

    -- Delete the file if it exists
    if fs:exists(file_path) then
        info_log("Deleting file", file_path)
        fs:remove(file_path)
        return true
    end

    return false
end

-- Track files that were written during this run
local written_files = {}

-- Get all directories recursively under base_dir
local function get_all_directories(fs, base_dir)
    local dirs = {}
    local function scan_dir(dir)
        if not fs:exists(dir) then
            return
        end

        table.insert(dirs, dir)
        for entry in fs:readdir(dir) do
            if entry.type == fs_module.type.DIR then
                scan_dir(dir .. "/" .. entry.name)
            end
        end
    end

    scan_dir(base_dir)
    return dirs
end

-- Convert filesystem path to namespace
local function path_to_namespace(path, base_dir)
    -- Remove base_dir from the beginning
    local rel_path = path:sub(#base_dir + 2) -- +2 to account for the trailing slash
    -- Replace slashes with dots
    local namespace = rel_path:gsub("/", ".")
    return namespace
end

-- Clean up orphaned files (files no longer referenced by any entry)
local function cleanup_orphaned_files(fs, base_dir, namespaces)
    local stats = {
        orphaned_files_removed = 0
    }

    -- Build a map of all valid referenced files
    local referenced_files = {}
    for ns, ns_data in pairs(namespaces) do
        for filename, _ in pairs(ns_data.referenced_files) do
            local path = base_dir .. "/" .. string.gsub(ns, "%.", "/") .. "/" .. filename
            referenced_files[path] = true
        end
    end

    -- Get all directories to scan
    local all_dirs = get_all_directories(fs, base_dir)

    -- Check each directory for files that should be removed
    for _, dir_path in ipairs(all_dirs) do
        -- Skip the base directory itself
        if dir_path ~= base_dir then
            -- List all files in this directory (excluding _index.yaml for now)
            for entry in fs:readdir(dir_path) do
                if entry.type == fs_module.type.FILE and entry.name ~= "_index.yaml" then
                    local file_path = dir_path .. "/" .. entry.name

                    -- If this file wasn't written in this run, and it's not referenced, delete it
                    if not written_files[file_path] and not referenced_files[file_path] then
                        info_log("Removing orphaned file", file_path)
                        local success, err = pcall(function() fs:remove(file_path) end)
                        if success then
                            stats.orphaned_files_removed = stats.orphaned_files_removed + 1
                        else
                            warn_log("Failed to remove orphaned file", {path = file_path, error = err})
                        end
                    end
                end
            end
        end
    end

    return stats
end

-- Clean up empty namespaces (directories with no entries)
local function cleanup_empty_namespaces(fs, base_dir, namespaces)
    local stats = {
        empty_namespaces_removed = 0,
        index_files_removed = 0
    }

    -- First, build a set of all namespaces that have entries
    local active_namespaces = {}
    for ns, ns_data in pairs(namespaces) do
        if #ns_data.entries > 0 then
            active_namespaces[ns] = true

            -- Also mark all parent namespaces as active
            local parent = ns
            while true do
                parent = parent:match("(.+)%.[^%.]+$")
                if not parent then break end
                active_namespaces[parent] = true
            end
        end
    end

    -- Get all directories to check
    local all_dirs = get_all_directories(fs, base_dir)

    -- Sort directories by depth (deepest first) to handle nested empty namespaces
    table.sort(all_dirs, function(a, b)
        local a_depth = select(2, a:gsub("/", ""))
        local b_depth = select(2, b:gsub("/", ""))
        return a_depth > b_depth
    end)

    -- Process each directory, starting from the deepest
    for _, dir_path in ipairs(all_dirs) do
        -- Skip the base directory itself
        if dir_path ~= base_dir then
            local namespace = path_to_namespace(dir_path, base_dir)

            -- If this namespace isn't active (no entries), clean it up
            if not active_namespaces[namespace] then
                -- First, check for and remove _index.yaml
                local index_path = dir_path .. "/_index.yaml"
                if fs:exists(index_path) then
                    info_log("Removing _index.yaml for empty namespace", namespace)
                    local success, err = pcall(function() fs:remove(index_path) end)
                    if success then
                        stats.index_files_removed = stats.index_files_removed + 1
                    else
                        warn_log("Failed to remove _index.yaml", {path = index_path, error = err})
                    end
                end

                -- Check if directory is now empty
                local is_empty = true
                for _ in fs:readdir(dir_path) do
                    is_empty = false
                    break
                end

                -- Try to remove the directory if it's empty
                if is_empty then
                    info_log("Removing empty namespace directory", dir_path)
                    local success, err = pcall(function() fs:remove(dir_path) end)
                    if success then
                        stats.empty_namespaces_removed = stats.empty_namespaces_removed + 1
                    else
                        warn_log("Failed to remove empty namespace directory", {path = dir_path, error = err})
                    end
                else
                    debug_log("Directory not empty, cannot remove", dir_path)

                    -- Additional check for orphaned files
                    for entry in fs:readdir(dir_path) do
                        if entry.type == fs_module.type.FILE and entry.name ~= "_index.yaml" then
                            local file_path = dir_path .. "/" .. entry.name
                            info_log("Found file in supposedly empty namespace, removing", file_path)
                            local success, err = pcall(function() fs:remove(file_path) end)
                            if not success then
                                warn_log("Failed to remove orphaned file", {path = file_path, error = err})
                            end
                        end
                    end

                    -- Check again if directory is now empty
                    is_empty = true
                    for _ in fs:readdir(dir_path) do
                        is_empty = false
                        break
                    end

                    -- Try to remove the directory if it's empty now
                    if is_empty then
                        info_log("Directory now empty, removing", dir_path)
                        local success, err = pcall(function() fs:remove(dir_path) end)
                        if success then
                            stats.empty_namespaces_removed = stats.empty_namespaces_removed + 1
                        else
                            warn_log("Failed to remove empty namespace directory", {path = dir_path, error = err})
                        end
                    end
                end
            end
        end
    end

    return stats
end

---------------------------
-- Core Functions
---------------------------

-- Download entries from registry to filesystem
function downloader.download(options)
    options = options or {}
    written_files = {} -- Reset tracking of written files

    info_log("Starting download from registry to filesystem", options)

    -- Get target directory from environment or options
    local base_dir, dir_err = get_target_directory(options)
    if not base_dir then
        error_log("Failed to determine target directory", dir_err)
        return {
            success = false,
            message = dir_err
        }
    end
    info_log("Using target directory", base_dir)

    -- Get filesystem id from environment or options
    local fs_id, fs_err = get_filesystem_id(options)
    if not fs_id then
        error_log("Failed to determine filesystem ID", fs_err)
        return {
            success = false,
            message = fs_err
        }
    end
    info_log("Using filesystem", fs_id)

    -- Get filesystem instance
    local fs = fs_module.get(fs_id)
    if not fs then
        error_log("Failed to get filesystem instance", fs_id)
        return {
            success = false,
            message = "Failed to get filesystem instance for '" .. fs_id .. "'"
        }
    end

    -- Ensure base directory exists
    if not fs:exists(base_dir) then
        debug_log("Creating base directory", base_dir)
        fs:mkdir(base_dir)
    end

    -- Get registry snapshot
    local snapshot, err = registry.snapshot()
    if not snapshot then
        error_log("Failed to get registry snapshot", err or "unknown error")
        return {
            success = false,
            message = "Failed to get registry snapshot: " .. (err or "unknown error")
        }
    end
    info_log("Got registry snapshot")

    -- Get all entries
    local all_entries, err = snapshot:entries()
    if err then
        error_log("Failed to get registry entries", err)
        return {
            success = false,
            message = "Failed to get registry entries: " .. err
        }
    end
    info_log("Retrieved entries from registry", #all_entries)

    -- Track namespaces and group entries by namespace
    local namespaces = {}
    local stats = {
        namespaces = 0,
        entries = 0,
        files = 0,
        files_skipped = 0,
        deleted = 0,
        orphaned_files_removed = 0,
        empty_namespaces_removed = 0,
        index_files_removed = 0
    }

    -- Handle deleted entries if provided
    if options.deleted_entries and #options.deleted_entries > 0 then
        info_log("Processing deleted entries", #options.deleted_entries)
        for _, entry in ipairs(options.deleted_entries) do
            -- Parse ID to extract namespace and name
            local ns, name = string.match(entry.id, "(.+):(.+)")
            if ns and name then
                debug_log("Processing deleted entry", {namespace = ns, name = name, kind = entry.kind})

                -- Get config for this kind
                local kind_config = downloader.KIND_CONFIG[entry.kind]
                if kind_config then
                    -- Delete source file if it exists
                    local deleted = delete_source_file(fs, base_dir, ns, entry, kind_config)
                    if deleted then
                        stats.deleted = stats.deleted + 1
                        info_log("Deleted file for entry", entry.id)
                    else
                        debug_log("No file to delete for entry", entry.id)
                    end
                else
                    debug_log("No kind config for deleted entry", entry.kind)
                end
            else
                warn_log("Invalid ID format for deleted entry", entry.id)
            end
        end
    end

    -- Group entries by namespace
    for _, entry in ipairs(all_entries) do
        -- Parse ID to extract namespace and name
        local ns, name = string.match(entry.id, "(.+):(.+)")
        if ns and name then
            -- Initialize namespace entry if not exists
            if not namespaces[ns] then
                namespaces[ns] = {
                    entries = {},
                    referenced_files = {} -- Track referenced files to detect orphans later
                }
                stats.namespaces = stats.namespaces + 1
                debug_log("Added new namespace", ns)
            end

            -- Create entry structure (preserving original structure)
            local yaml_entry = {
                name = name,
                kind = entry.kind
            }

            -- Preserve meta as a nested structure
            if entry.meta then
                yaml_entry.meta = entry.meta
            end

            -- Copy all data fields to yaml_entry
            if entry.data then
                for k, v in pairs(entry.data) do
                    yaml_entry[k] = v
                end
            end

            -- Get config for this kind
            local kind_config = downloader.KIND_CONFIG[entry.kind]
            local config = nil

            -- Check if we have a direct kind config or need to check meta.type
            if kind_config then
                if kind_config.types and entry.meta and entry.meta.type and
                    kind_config.types[entry.meta.type] then
                    -- Use type-specific config
                    config = kind_config.types[entry.meta.type]
                else
                    -- Use direct kind config
                    config = kind_config
                end
            end

            -- Handle file materialization if we have a config
            if config and config.source_field and config.extension then
                local source_field = config.source_field
                local extension = config.extension

                -- Check if the source field exists and isn't already a file:// URL
                if yaml_entry[source_field] and type(yaml_entry[source_field]) == "string" and
                    not yaml_entry[source_field]:match("^file://") then
                    -- Prepare directory path
                    local dir_path = ensure_directory(fs, base_dir, ns)

                    -- Determine filename
                    local filename = name
                    if not filename:match(extension .. "$") then
                        filename = filename .. extension
                    end

                    -- Write file only if content differs or file doesn't exist
                    local file_path = dir_path .. "/" .. filename
                    if should_write_file(fs, file_path, yaml_entry[source_field]) then
                        debug_log("Writing file", file_path)
                        fs:write_file(file_path, yaml_entry[source_field])
                        written_files[file_path] = true
                        stats.files = stats.files + 1
                    else
                        stats.files_skipped = stats.files_skipped + 1
                    end

                    -- Update to file reference and track the reference
                    yaml_entry[source_field] = "file://" .. filename
                    namespaces[ns].referenced_files[filename] = true
                    trace_log("Tracking referenced file", {namespace = ns, filename = filename})
                elseif yaml_entry[source_field] and type(yaml_entry[source_field]) == "string" and
                    yaml_entry[source_field]:match("^file://") then
                    -- Extract the filename and track it as referenced
                    local filename = extract_filename(yaml_entry[source_field])
                    if filename then
                        namespaces[ns].referenced_files[filename] = true
                        trace_log("Tracking existing file reference", {namespace = ns, filename = filename})
                    end
                end
            end

            -- Add to namespace entries
            table.insert(namespaces[ns].entries, yaml_entry)
            stats.entries = stats.entries + 1
        else
            warn_log("Invalid ID format, skipping entry", entry.id)
        end
    end

    -- Write namespace files
    for ns, namespace_data in pairs(namespaces) do
        -- Skip empty namespaces (no entries)
        if #namespace_data.entries == 0 then
            info_log("Skipping empty namespace", ns)
            goto continue
        end

        -- Prepare directory path
        local dir_path = ensure_directory(fs, base_dir, ns)

        -- Generate only _index.yaml file
        local index_filepath = dir_path .. "/_index.yaml"

        -- Sort entries by name
        table.sort(namespace_data.entries, function(a, b)
            return a.name < b.name
        end)

        -- Generate the header content (namespace, version, meta)
        local header = {
            namespace = ns,
            version = "1.0"
        }

        -- Add meta section if it exists
        if namespace_data.meta then
            header.meta = namespace_data.meta
        end

        -- Define yaml options according to spec
        local yaml_options = {
            indent = 2,                -- 2-space indentation
            field_order = downloader.FIELD_ORDER, -- Field order according to our priority list
            sort_unordered = true      -- Sort remaining fields alphabetically
        }

        -- Generate YAML for header
        local header_yaml, err = yaml.encode(header, yaml_options)
        if err then
            error_log("Failed to encode YAML header", {namespace = ns, error = err})
            return {
                success = false,
                message = "Failed to encode YAML header: " .. err
            }
        end

        -- Start with the header content
        local content = header_yaml

        -- Add blank line and entries section header
        content = content .. "\n" .. "entries:" .. "\n"

        -- Process each entry individually
        for i, entry in ipairs(namespace_data.entries) do
            -- Generate YAML for a single entry
            local entry_yaml, err = yaml.encode(entry, yaml_options)
            if err then
                error_log("Failed to encode entry", {namespace = ns, name = entry.name, error = err})
                return {
                    success = false,
                    message = "Failed to encode entry: " .. err
                }
            end

            local label = ns .. ":" .. entry.name

            -- Format the entry with proper indentation
            -- The entry is currently formatted without the leading "- ", so add it
            entry_yaml = "  # " .. label .. "\n" .. "  - " .. entry_yaml:gsub("\n", "\n    ")

            -- Trim any trailing newlines
            entry_yaml = entry_yaml:gsub("[\n\r]+$", "")

            -- Add the entry to the content
            content = content .. entry_yaml

            -- Add blank line between entries (except after the last one)
            if i < #namespace_data.entries then
                content = content .. "\n"
            end
        end

        -- Write the file if content differs or file doesn't exist
        if should_write_file(fs, index_filepath, content) then
            debug_log("Writing index file", index_filepath)
            fs:write_file(index_filepath, content)
            written_files[index_filepath] = true
        else
            debug_log("Index file unchanged, skipping write", index_filepath)
        end

        ::continue::
    end

    -- Clean up orphaned files and empty namespaces (if enabled)
    if options.cleanup_orphaned ~= false then
        info_log("Checking for orphaned files")
        local cleanup_stats = cleanup_orphaned_files(fs, base_dir, namespaces)
        stats.orphaned_files_removed = cleanup_stats.orphaned_files_removed

        -- Clean up empty namespaces
        info_log("Checking for empty namespaces")
        local empty_ns_stats = cleanup_empty_namespaces(fs, base_dir, namespaces)
        stats.empty_namespaces_removed = empty_ns_stats.empty_namespaces_removed
        stats.index_files_removed = empty_ns_stats.index_files_removed
    end

    info_log("Download completed", stats)
    return {
        success = true,
        message = "Registry successfully reorganized into file structure",
        stats = stats
    }
end

-- Download specific entries
function downloader.download_entries(entries, options)
    options = options or {}
    written_files = {} -- Reset tracking of written files

    info_log("Starting download of specific entries", #entries)

    -- Get target directory from environment or options
    local base_dir, dir_err = get_target_directory(options)
    if not base_dir then
        error_log("Failed to determine target directory", dir_err)
        return {
            success = false,
            message = dir_err
        }
    end
    info_log("Using target directory", base_dir)

    -- Get filesystem id from environment or options
    local fs_id, fs_err = get_filesystem_id(options)
    if not fs_id then
        error_log("Failed to determine filesystem ID", fs_err)
        return {
            success = false,
            message = fs_err
        }
    end
    info_log("Using filesystem", fs_id)

    -- Get filesystem instance
    local fs = fs_module.get(fs_id)
    if not fs then
        error_log("Failed to get filesystem instance", fs_id)
        return {
            success = false,
            message = "Failed to get filesystem instance for '" .. fs_id .. "'"
        }
    end

    -- Ensure base directory exists
    if not fs:exists(base_dir) then
        debug_log("Creating base directory", base_dir)
        fs:mkdir(base_dir)
    end

    local stats = {
        namespaces = 0,
        entries = 0,
        files = 0,
        files_skipped = 0
    }

    local processed_namespaces = {}

    -- Process each entry
    for _, entry in ipairs(entries) do
        -- Parse ID to extract namespace and name
        local ns, name = string.match(entry.id, "(.+):(.+)")
        if ns and name then
            debug_log("Processing entry", {namespace = ns, name = name, kind = entry.kind})

            -- Track namespace
            if not processed_namespaces[ns] then
                processed_namespaces[ns] = true
                stats.namespaces = stats.namespaces + 1
                debug_log("Added new namespace", ns)
            end

            -- Get config for this kind
            local kind_config = downloader.KIND_CONFIG[entry.kind]
            local config = nil

            -- Check if we have a direct kind config or need to check meta.type
            if kind_config then
                if kind_config.types and entry.meta and entry.meta.type and
                    kind_config.types[entry.meta.type] then
                    -- Use type-specific config
                    config = kind_config.types[entry.meta.type]
                else
                    -- Use direct kind config
                    config = kind_config
                end
            end

            -- Handle file materialization if we have a config
            if config and config.source_field and config.extension then
                local source_field = config.source_field
                local extension = config.extension

                -- Check if the source field exists and isn't already a file:// URL
                if entry[source_field] and type(entry[source_field]) == "string" and
                    not entry[source_field]:match("^file://") then
                    -- Prepare directory path
                    local dir_path = ensure_directory(fs, base_dir, ns)

                    -- Determine filename
                    local filename = name
                    if not filename:match(extension .. "$") then
                        filename = filename .. extension
                    end

                    -- Write file only if content differs or file doesn't exist
                    local file_path = dir_path .. "/" .. filename
                    if should_write_file(fs, file_path, entry[source_field]) then
                        debug_log("Writing file", file_path)
                        fs:write_file(file_path, entry[source_field])
                        written_files[file_path] = true
                        stats.files = stats.files + 1
                    else
                        stats.files_skipped = stats.files_skipped + 1
                    end
                end
            else
                trace_log("No applicable config for file materialization", {kind = entry.kind, meta_type = entry.meta and entry.meta.type})
            end

            stats.entries = stats.entries + 1
        else
            warn_log("Invalid ID format, skipping entry", entry.id)
        end
    end

    info_log("Download of specific entries completed", stats)
    return {
        success = true,
        message = "Entries successfully written to filesystem",
        stats = stats
    }
end

-- Check if a file needs to be deleted (referenced by file:// but no longer in the registry)
function downloader.check_for_orphaned_files(options)
    options = options or {}

    info_log("Checking for orphaned files", options)

    -- Get target directory from environment or options
    local base_dir, dir_err = get_target_directory(options)
    if not base_dir then
        error_log("Failed to determine target directory", dir_err)
        return {
            success = false,
            message = dir_err
        }
    end
    info_log("Using target directory", base_dir)

    -- Get filesystem id from environment or options
    local fs_id, fs_err = get_filesystem_id(options)
    if not fs_id then
        error_log("Failed to determine filesystem ID", fs_err)
        return {
            success = false,
            message = fs_err
        }
    end
    info_log("Using filesystem", fs_id)

    -- Get filesystem instance
    local fs = fs_module.get(fs_id)
    if not fs then
        error_log("Failed to get filesystem instance", fs_id)
        return {
            success = false,
            message = "Failed to get filesystem instance for '" .. fs_id .. "'"
        }
    end

    -- Get registry snapshot
    local snapshot, err = registry.snapshot()
    if not snapshot then
        error_log("Failed to get registry snapshot", err or "unknown error")
        return {
            success = false,
            message = "Failed to get registry snapshot: " .. (err or "unknown error")
        }
    end
    info_log("Got registry snapshot")

    -- Get all entries
    local all_entries, err = snapshot:entries()
    if err then
        error_log("Failed to get registry entries", err)
        return {
            success = false,
            message = "Failed to get registry entries: " .. err
        }
    end
    info_log("Retrieved entries from registry", #all_entries)

    -- Track all file:// references
    local referenced_files = {}
    local namespaces = {}

    -- Collect all file:// references from entries
    for _, entry in ipairs(all_entries) do
        local ns, name = string.match(entry.id, "(.+):(.+)")
        if ns and name then
            -- Initialize namespace if needed
            if not namespaces[ns] then
                namespaces[ns] = true
            end

            -- Get config for this kind
            local kind_config = downloader.KIND_CONFIG[entry.kind]
            local config = nil

            -- Check if we have a direct kind config or need to check meta.type
            if kind_config then
                if kind_config.types and entry.meta and entry.meta.type and
                    kind_config.types[entry.meta.type] then
                    -- Use type-specific config
                    config = kind_config.types[entry.meta.type]
                else
                    -- Use direct kind config
                    config = kind_config
                end
            end

            -- Check for file:// references
            if config and config.source_field then
                local source_field = config.source_field

                if entry[source_field] and type(entry[source_field]) == "string" then
                    local filename = extract_filename(entry[source_field])
                    if filename then
                        local file_path = ns .. "/" .. filename
                        referenced_files[file_path] = true
                        trace_log("Found referenced file", file_path)
                    end
                end
            end
        end
    end

    -- List of orphaned files (files that exist but aren't referenced)
    local orphaned_files = {}

    -- Scan filesystem for files that aren't referenced
    local all_dirs = get_all_directories(fs, base_dir)
    for _, dir_path in ipairs(all_dirs) do
        -- Skip the base directory itself
        if dir_path ~= base_dir then
            local namespace = path_to_namespace(dir_path, base_dir)

            -- Skip if directory doesn't exist
            if not fs:exists(dir_path) then
                debug_log("Directory doesn't exist, skipping", dir_path)
                goto continue
            end

            -- Check files in this directory
            for entry in fs:readdir(dir_path) do
                if entry.type == fs_module.type.FILE and entry.name ~= "_index.yaml" then
                    local file_path = namespace .. "/" .. entry.name
                    if not referenced_files[file_path] then
                        table.insert(orphaned_files, {
                            namespace = namespace,
                            filename = entry.name,
                            path = dir_path .. "/" .. entry.name
                        })
                        debug_log("Found orphaned file", file_path)
                    end
                end
            end

            ::continue::
        end
    end

    info_log("Orphaned file check completed", {
        referenced = #referenced_files,
        orphaned = #orphaned_files
    })

    return {
        success = true,
        referenced_files = referenced_files,
        orphaned_files = orphaned_files
    }
end

return downloader