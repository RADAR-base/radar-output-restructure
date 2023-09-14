package org.radarbase.output.source

interface StorageIndexManager {
    val storageIndex: StorageIndex

    suspend fun update()
    suspend fun sync()
}
