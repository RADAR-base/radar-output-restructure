package org.radarbase.output.source

class InMemoryStorageIndexFactory : StorageIndexFactory {
    override fun get(): StorageIndex = InMemoryStorageIndex()
}
