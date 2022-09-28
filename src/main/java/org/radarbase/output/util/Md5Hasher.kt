package org.radarbase.output.util

import java.security.MessageDigest
import java.util.*

class Md5Hasher {
    private val md5 = MessageDigest.getInstance("MD5")
    private val base64 = Base64.getEncoder().withoutPadding()

    fun hash(value: Array<String>): String {
        value.forEachIndexed { i, field ->
            if (i > 0) {
                md5.update(','.code.toByte())
            }
            md5.update(field.toByteArray())
        }
        val hash = base64.encodeToString(md5.digest())
        md5.reset()
        return hash
    }
}
