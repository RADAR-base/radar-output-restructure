package org.radarbase.output.config

data class LocalConfig(
    /** User ID (uid) to write data as. Only valid on Unix-based filesystems. */
    val userId: Int = -1,
    /** Group ID (gid) to write data as. Only valid on Unix-based filesystems. */
    val groupId: Int = -1,
)
