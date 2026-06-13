package org.radarbase.output.format

/** Raised when an Avro record cannot be converted to ODM. Signals [RecordConverter.writeRecord] to retry another path. */
internal class OdmConversionException(message: String) : IllegalStateException(message)
