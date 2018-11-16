# GCSObjectStores 

This package defines an object storage client that uses Google Cloud Storage as the storage back-end.

It is used as the storage client in an `ObjectStore` instance (see the [ObjectStores]() package for details).

It is a concrete subtype of `ObjectStores.ObjectStoreClient`.


storage(:Object, :list, "abc123foozzz")

storage(:Object, :insert, "abc123foozzz";
    name="myobject",
    data="here is some content",
    content_type="text/plain"   # Need mime type
)

obs = storage(:Object, :list, "abc123foozzz")

s = storage(:Object, :get, "abc123foozzz", "myobject");

storage(:Object, :delete, "abc123foozzz", "myobject")

storage(:Bucket, :delete, "abc123foozzz")
