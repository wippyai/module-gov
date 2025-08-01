local logger = require("logger")
local registry = require("registry")

-- Create a named logger for this library
local log = logger:named("wippy.gov.lua.deps.lib")

-- Create the library table
local lib = {}

-- Helper function to extract registry namespace from a module path
function lib.extract_namespace(module)
    if module and module:find(":") then
        local namespace = module:match("^([^:]+):")
        return namespace
    end
    return nil
end

-- Helper function to extract module name from a registry module
function lib.extract_module_name(module)
    if module and module:find(":") then
        local name = module:match(":([^:]+)$")
        return name
    end
    return module
end

-- Helper function to determine if a module is a registry module
function lib.is_registry_module(module)
    return module and module:find(":") ~= nil
end

-- Check if a module exists in the registry
function lib.module_exists_in_registry(module)
    if not lib.is_registry_module(module) then
        return false
    end

    -- Try to get the entry from registry
    local entry, err = registry.get(module)
    return entry ~= nil
end

-- Check if a module exists in a specific namespace
function lib.module_exists_in_namespace(module_name, namespace)
    if not module_name or not namespace then
        return false
    end

    -- Form the full module path
    local full_module = namespace .. ":" .. module_name

    -- Check if it exists
    return lib.module_exists_in_registry(full_module)
end

-- Find missing modules in the entry
function lib.find_missing_dependencies(requires, declared_modules, declared_imports, base_namespace)
    local missing_modules = {}         -- Direct module requires
    local missing_registry = {}        -- Registry modules with namespaces
    local missing_local_modules = {}   -- Local namespace modules

    if not requires then
        return {
            modules = missing_modules,
            registry = missing_registry,
            local_modules = missing_local_modules
        }
    end

    for module, _ in pairs(requires) do
        -- Skip if already declared
        if declared_modules[module] or declared_imports[module] then
            goto continue
        end

        -- Check if it's a registry module
        if lib.is_registry_module(module) then
            -- Check if it exists in registry
            if lib.module_exists_in_registry(module) then
                table.insert(missing_registry, module)
            else
                -- Module with namespace but doesn't exist
                log:warn("Registry module not found: " .. module)
                table.insert(missing_registry, module)  -- Still add it as missing
            end
        else
            -- Module without namespace
            -- Check if it exists in the base namespace
            if base_namespace and lib.module_exists_in_namespace(module, base_namespace) then
                table.insert(missing_local_modules, module)
            else
                table.insert(missing_modules, module)
            end
        end

        ::continue::
    end

    return {
        modules = missing_modules,
        registry = missing_registry,
        local_modules = missing_local_modules
    }
end

return lib