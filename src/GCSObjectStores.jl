module GCSObjectStores

using GoogleCloud
using JSON

using ObjectStores:  ObjectStoreClient
using ObjectStores:  @add_required_fields_storageclient
using Authorization: AbstractResource
using Authorization: @add_required_fields_resource


################################################################################
# Types

struct Bucket <: AbstractResource
    @add_required_fields_resource  # id
end

struct Object <: AbstractResource
    @add_required_fields_resource  # id
end

struct Client <: ObjectStoreClient
    @add_required_fields_storageclient  # :bucket_type, :object_type
    storage::GoogleCloud.api.APIRoot

    function Client(bucket_type, object_type, storage)
        !(bucket_type == Bucket) && error("GCSObjectStores.bucket_type must be Bucket.")
        !(object_type == Object) && error("GCSObjectStores.object_type must be Object.")
        new(bucket_type, object_type, storage)
    end
end

function Client(filename::String)
    creds   = GoogleCloud.JSONCredentials(filename)
    session = GoogleSession(creds, ["devstorage.full_control"])
    set_session!(storage, session)
    Client(Bucket, Object, storage)
end


################################################################################
# Buckets

"Create bucket. If successful return nothing, else return an error message as a String."
function _create!(client::ObjectStore{GCSObjectStores.Client}, bucket::Bucket)
    storage = client.storageclient.storage
    res = storage(:Bucket, :insert; data=Dict(:name => bucket.id))
    if isempty(res)  # UInt8[]
        return "Bucket already exists. Cannot create it again."
    else
        res = JSON.parse(replace(String(res), "\n" => ""))
        if haskey(res, "name") && res["name"] == bucket.id && haskey(res, "timeCreated")
            return nothing  # Success
        else
            return ""
        end
    end
#=
    _isbucket(bucket.id) && return "Bucket already exists. Cannot create it again."
    cb, bktname = splitdir(bucket.id)
    !_isbucket(cb) && return "Cannot create bucket within a non-existent bucket."
    try
        mkdir(bucket.id)
        return nothing
    catch e
        return e.prefix  # Assumes e is a SystemError
    end
=#
end


"Read bucket. If successful return (true, value), else return (false, errormessage::String)."
function _read(client::ObjectStore{GCSObjectStores.Client}, bucket::Bucket)
#=
1. storage(:Bucket, :list; raw=true) returns addition information.
2. Result is a Vector{UInt8} of almost-JSON...there are many newline characters
=#
#=
bkts = storage(:Bucket, :list)  # Vector{UInt8}. List of all buckets for the project
bkts = JSON.parse(replace(String(bkts), "\n" => ""))

bkts["items"]
bkt = bkts["items"][3]
contents = storage(:Object, :list, bkt["name"])
storage(:Bucket, :get, "jobs.h27n.tech")  # metadata for bucket "jobs.h27n.tech"
=#
    storage  = client.storageclient.storage
    contents = storage(:Object, :list, bucket.id)
    contents = JSON.parse(replace(String(contents), "\n" => ""))
    if haskey(contents, "items")
        return (true, [x["name"] for x in contents["items"]])
    else
        return (false, "Bucket doesn't exist")
    end
end


"Delete bucket. If successful return nothing, else return an error message as a String."
function _delete!(client::ObjectStore{GCSObjectStores.Client}, bucket::Bucket)
    ok, contents = _read(bucket)
    contents == nothing && return "Resource is not a bucket. Cannot delete it with this function."
    !isempty(contents)  && return "Bucket is not empty. Cannot delete it."
    try
        rm(bucket.id)
        return nothing
    catch e
        return e.prefix  # Assumes e is a SystemError
    end
end


################################################################################
# Objects

"Create object. If successful return nothing, else return an error message as a String."
function _create!(client::ObjectStore{GCSObjectStores.Client}, object::Object, v)
    try
        resourceid = object.id
        _isbucket(resourceid) && return "$(resourceid) is a bucket, not an object"
        cb, shortname = splitdir(resourceid)
        !_isbucket(cb) && return "Cannot create object $(resourceid) inside a non-existent bucket."
        write(object.id, v)
        return nothing
    catch e
        return e.prefix  # Assumes e is a SystemError
    end
end


"Read object. If successful return (true, value), else return (false, errormessage::String)."
function _read(client::ObjectStore{GCSObjectStores.Client}, object::Object)
    !_isobject(object.id) && return (false, "Object ID does not refer to an existing object")
    try
        true, read(object.id)
    catch e
        return false, e.prefix  # Assumes e is a SystemError
    end
end


"Delete object. If successful return nothing, else return an error message as a String."
function _delete!(client::ObjectStore{GCSObjectStores.Client}, object::Object)
    !_isobject(object.id) && return "Object ID does not refer to an existing object. Cannot delete a non-existent object."
    try
        rm(object.id)
        return nothing
    catch e
        return e.prefix  # Assumes e is a SystemError
    end
end


################################################################################
# Conveniences

_islocal(backend::Client) = false

_isbucket(resourceid::String) = isdir(resourceid)

_isobject(resourceid::String) = isfile(resourceid)

end # module
