# GCSObjectStores 

This package defines an object storage client that uses Google Cloud Storage as the storage back-end.

It is used as the storage client in an `ObjectStore` instance (see the [ObjectStores]() package for details).

It is a concrete subtype of `ObjectStores.ObjectStoreClient`.


__NOTE:__ Google Cloud Storage does not allow the creation of buckets within buckets. Buckets can contain only objects.
