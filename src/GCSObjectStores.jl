module GCSObjectStores

using Authorization
using GoogleCloud
using JSON
using ObjectStores


################################################################################
# Types

struct GCSObjectStore <: ObjectStore
    id::String;
    id2permission::Dict{String, Permission};        # Resource ID => Permission
    idpattern2permission::Dict{Regex, Permission};  # Resource ID pattern => Permission
    type2permission::Dict{DataType, Permission};    # Resource type => Permission
    rootbucketID::String                            # ID of root bucket
    storage::GoogleCloud.api.APIRoot

    function GCSObjectStore(id, id2permission, idpattern2permission, type2permission, rootbucketID, storage)
        newstore = new(id, id2permission, idpattern2permission, type2permission, rootbucketID, storage)
        _isobject(newstore, rootbucketID) && error("Root already exists as an object. Cannot use it as a bucket.")
        if !_isbucket(newstore, rootbucketID)  # Root does not exist...create it
            msg = createbucket!(newstore)        # One arg implies bucketname is root
            msg != nothing && @warn msg          # Couldn't create root bucket...warn
        end
        newstore
    end
end


function GCSObjectStore(filename::String, rootbucketID::String)
    id = ""
    id2permission        = Dict{String, Permission}()
    idpattern2permission = Dict{Regex,  Permission}()
    type2permission      = Dict{DataType, Permission}()
    creds   = GoogleCloud.JSONCredentials(filename)
    session = GoogleSession(creds, ["devstorage.full_control"])
    set_session!(storage, session)
    GCSObjectStore(id, id2permission, idpattern2permission, type2permission, rootbucketID, storage)
end


################################################################################
# Buckets

"Create bucket. If successful return nothing, else return an error message as a String."
function _create!(client::GCSObjectStore, bucket::Bucket)
    res = client.storage(:Bucket, :insert; data=Dict(:name => bucket.id))
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
end


"Read bucket. If successful return (true, value), else return (false, errormessage::String)."
function _read(client::GCSObjectStore, bucket::Bucket)
    contents = try
        client.storage(:Object, :list, bucket.id)
    catch e
        nothing
    end
    contents == nothing && return (false, "Bucket doesn't exist")
    contents = JSON.parse(replace(String(contents), "\n" => ""))
    if isempty(contents)  # UInt8[]
        return (false, "Bucket doesn't exist")
    elseif haskey(contents, "items")
        return (true, [x["name"] for x in contents["items"]])
    else
        return (true, String[])  # Bucket is empty
    end
end


"Delete bucket. If successful return nothing, else return an error message as a String."
function _delete!(client::GCSObjectStore, bucket::Bucket)
    ok, contents = _read(client, bucket)
    !ok && return "Resource is not a bucket. Cannot delete it with this function."
    !isempty(contents)  && return "Bucket is not empty. Cannot delete it."
    try
        client.storage(:Bucket, :delete, bucket.id)
        return nothing
    catch e
        return "$(e)"
    end
end


################################################################################
# Objects

"""
Create object. If successful return nothing, else return an error message as a String.

v is either:
- NamedTuple: (mimetype="application.json", value="some value")
- An arbitrary value
"""
function _create!(client::GCSObjectStore, object::Object, v)
    bucketname, objectname = splitdir(object.id)
    mimetype, val = typeof(v) <: NamedTuple ? v[:mimetype], v[:value] : "application/json", v
    try
        res = client.storage(:Object, :insert, bucketname;
            name=objectname,
            data=val,
            content_type=mimetype
        )
        return nothing
    catch e
        return "Cannot create object $(object.id) inside a non-existent bucket."
    end
end


"Read object. If successful return (true, value), else return (false, errormessage::String)."
function _read(client::GCSObjectStore, object::Object)
    bucketname, objectname = splitdir(object.id)
    try
        val = client.storage(:Object, :get, bucketname, objectname)
        return true, val
    catch e
        return (false, "Object ID does not refer to an existing object")
    end
end


"Delete object. If successful return nothing, else return an error message as a String."
function _delete!(client::GCSObjectStore, object::Object)
    try
        bucketname, objectname = splitdir(object.id)
        res = client.storage(:Object, :delete, bucketname, objectname)
        return nothing
    catch e
        return "Object ID does not refer to an existing object. Cannot delete a non-existent object."
    end
end


################################################################################
# Conveniences

function _isbucket!(client::GCSObjectStore, resourceid::String)
    try
        res = client.storage(:Bucket, :get, resourceid)  # Bucket metadata
        return true
    catch e
        return false
    end
end


function _isobject!(client::GCSObjectStore, resourceid::String)
    try
        bucketname, objectname = splitdir(resourceid)
        res = client.storage(:Object, :get, bucketname, objectname)  # Bucket metadata
        return true
    catch e
        return false
    end
end

end # module
